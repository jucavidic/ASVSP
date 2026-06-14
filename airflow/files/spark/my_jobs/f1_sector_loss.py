import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min, round, lit
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Sector_Loss") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    laps     = spark.read.parquet(base + "laps_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers  = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    drivers_dedup = drivers \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    sessions_race = sessions.filter(
                        (col("meeting_key")  == 1265) &
                        (col("session_name") == "Race")
                    ).select("session_key", "session_name", "meeting_key")

    df_filtered = laps \
                  .join(sessions_race, "session_key") \
                  .join(drivers_dedup, "driver_number") \
                  .filter(col("team_name") == "Ferrari") \
                  .drop(sessions_race["meeting_key"])

    # rolling avg of last 10 laps per driver - one value per lap
    window_last10 = Window.partitionBy("driver_number") \
                          .orderBy("lap_number") \
                          .rowsBetween(-10, 0)

    # best sector across all laps per driver - constant per driver, used to compute loss
    window_full = Window.partitionBy("driver_number") \
                        .orderBy("lap_number") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df_per_lap = df_filtered \
        .withColumn("rolling_avg_s1", avg("duration_sector_1").over(window_last10)) \
        .withColumn("rolling_avg_s2", avg("duration_sector_2").over(window_last10)) \
        .withColumn("rolling_avg_s3", avg("duration_sector_3").over(window_last10)) \
        .withColumn("best_s1", min("duration_sector_1").over(window_full)) \
        .withColumn("best_s2", min("duration_sector_2").over(window_full)) \
        .withColumn("best_s3", min("duration_sector_3").over(window_full)) \
        .withColumn("best_lap", min("lap_duration").over(window_full))

    # loss = rolling_avg - best (how much time is the driver losing vs their own best, at each lap)
    result_per_lap = df_per_lap \
        .withColumn("loss_s1", round(col("rolling_avg_s1") - col("best_s1"), 3)) \
        .withColumn("loss_s2", round(col("rolling_avg_s2") - col("best_s2"), 3)) \
        .withColumn("loss_s3", round(col("rolling_avg_s3") - col("best_s3"), 3)) \
        .withColumn("total_loss", round(col("loss_s1") + col("loss_s2") + col("loss_s3"), 3)) \
        .withColumn("meeting_key", lit(1265)) \
        .select(
            "driver_number",
            "full_name",
            "lap_number",
            "lap_duration",
            "best_lap",
            round("duration_sector_1", 3).alias("s1"),
            round("duration_sector_2", 3).alias("s2"),
            round("duration_sector_3", 3).alias("s3"),
            round("rolling_avg_s1",    3).alias("rolling_avg_s1"),
            round("rolling_avg_s2",    3).alias("rolling_avg_s2"),
            round("rolling_avg_s3",    3).alias("rolling_avg_s3"),
            "best_s1",
            "best_s2",
            "best_s3",
            "loss_s1",
            "loss_s2",
            "loss_s3",
            "total_loss",
            "meeting_key",
        ) \
        .orderBy("driver_number", "lap_number")

    print(">> Sector loss and degradation for Ferrari drivers in SPA 2025 <<")
    result_per_lap.show(60, truncate=False)

    result_per_lap.write.format("mongodb") \
        .mode("overwrite") \
        .option("database",   mongo_db) \
        .option("collection", "f1_sector_loss") \
        .save()

    print("Sector loss and degradation computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()