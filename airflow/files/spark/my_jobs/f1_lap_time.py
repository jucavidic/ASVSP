import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, min as spark_min, lit, round, first, last, round
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
            .appName("F1_Lap_Time") \
            .config("spark.mongodb.write.connection.uri", mongo_host) \
            .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    laps     = spark.read.parquet(base + "laps_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers  = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    sessions_filtered = sessions.filter(
                            (col("meeting_key")  == 1268) &
                            (col("session_name") == "Race")
                        )

    df = laps \
        .join(sessions_filtered, "session_key") \
        .join(drivers.select("driver_number", "full_name", "team_name"), "driver_number") \
        .filter(col("lap_duration").isNotNull())

    window_lap_session = Window.partitionBy("session_key", "lap_number")

    window_driver_rolling = Window \
                            .partitionBy("driver_number", "session_key") \
                            .orderBy("lap_number") \
                            .rowsBetween(-5, 0)

    #chronologically order laps
    window_driver_full = Window \
        .partitionBy("driver_number", "session_key") \
        .orderBy("lap_number") \
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    # session avg lap time -> avg of all drivers by lap
    # delta (by lap) -> negative means faster, positive means slower
    df_with_windows = df \
                    .withColumn("session_avg_lap_time",     avg("lap_duration").over(window_lap_session)) \
                    .withColumn("delta_to_session",    col("lap_duration") - col("session_avg_lap_time")) \
                    .withColumn("driver_rolling5_avg", avg("lap_duration").over(window_driver_rolling)) \
                    .withColumn("final_rolling5",      last("driver_rolling5_avg").over(window_driver_full))

    df_ferrari = df_with_windows.filter(col("team_name") == "Ferrari")


    result = df_ferrari.groupBy("driver_number", "full_name") \
                .agg(
                    avg("lap_duration").alias("avg_lap"),
                    spark_min("lap_duration").alias("best_lap"),
                    avg("delta_to_session").alias("avg_delta_to_field"),
                    first("final_rolling5").alias("rolling5_last"),
                ) \
                .withColumn("meeting_key", lit(1268)) \
                .withColumn("avg_lap", round(col("avg_lap"), 2)) \
                .withColumn("avg_delta_to_field", round(col("avg_delta_to_field"), 2)) \
                .withColumn("rolling5_last", round(col("rolling5_last"), 2)) \
                .orderBy("avg_lap")

    result.write.format("mongodb") \
                .mode("overwrite") \
                .option("database",   mongo_db) \
                .option("collection", "f1_lap_time") \
                .save()

    print("Lap time computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()
