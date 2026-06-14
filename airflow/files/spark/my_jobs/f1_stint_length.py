import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, min as spark_min, max as spark_max,
    row_number, first, last, lit, round, count
)
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Stint_Length") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    stints = spark.read.parquet(base + "stints_race_only_2025.parquet")
    laps = spark.read.parquet(base + "laps_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    sessions_race = sessions.filter(
                        (col("meeting_key")  == 1269) &  # Baku
                        (col("session_name") == "Race")
                    ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .filter(col("meeting_key") == 1269) \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    stints_base = stints \
                    .join(sessions_race, "session_key") \
                    .join(drivers_race, "driver_number") \
                    .filter(col("team_name") == "Ferrari")

    laps_base = laps \
                .join(sessions_race, "session_key") \
                .join(drivers_race, "driver_number") \
                .filter(col("team_name") == "Ferrari") \
                .filter(col("lap_duration").isNotNull())

    laps_with_stint = laps_base.alias("l").join(
                        stints_base.select(
                            col("driver_number").alias("s_drv"),
                            col("session_key").alias("s_sess"),
                            col("stint_number"),
                            col("compound"),
                            col("lap_start"),
                            col("lap_end"),
                        ).alias("s"),
                        (col("l.driver_number") == col("s.s_drv")) &
                        (col("l.session_key")   == col("s.s_sess")) &
                        (col("l.lap_number")    >= col("s.lap_start")) &
                        (col("l.lap_number")    <= col("s.lap_end")),
                        "left"
                    )

    # chronologically ordered by lap_number
    window_stint = Window \
                    .partitionBy("l.driver_number", "l.session_key", "stint_number") \
                    .orderBy("l.lap_number")

    window_stint_full = Window \
                        .partitionBy("l.driver_number", "l.session_key", "stint_number") \
                        .orderBy("l.lap_number") \
                        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # get lap_time degradation by lap, and last lap for stint before pit
    laps_windowed = laps_with_stint \
                    .withColumn("lap_in_stint", row_number().over(window_stint)) \
                    .withColumn(
                        "time_loss_vs_stint_start",
                        col("l.lap_duration") - first("l.lap_duration").over(window_stint_full)
                    ) \
                    .withColumn("last_lap_time", last("l.lap_duration").over(window_stint_full))

    result = laps_windowed.groupBy(
                    col("l.driver_number").alias("driver_number"),
                    "full_name", "stint_number", "compound"
                ).agg(
                    count("*").alias("laps_in_stint"),
                    avg("l.lap_duration").alias("avg_lap_time"),
                    spark_min("l.lap_duration").alias("best_lap_time"),
                    spark_max("time_loss_vs_stint_start").alias("max_degradation"),
                    avg("time_loss_vs_stint_start").alias("avg_degradation"),
                    first("last_lap_time").alias("last_lap_time"),
                ) \
                .withColumn("avg_lap_time", round(col("avg_lap_time"), 3)) \
                .withColumn("max_degradation", round(col("max_degradation"), 3)) \
                .withColumn("avg_degradation", round(col("avg_degradation"), 3)) \
                .withColumn("meeting_key", lit(1269)) \
                .orderBy("driver_number", "stint_number")

    result.write.format("mongodb") \
                .mode("overwrite") \
                .option("database",   mongo_db) \
                .option("collection", "f1_stint_length") \
                .save()

    print("Stint length computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()