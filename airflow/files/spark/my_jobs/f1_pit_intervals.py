import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, lead, lit, round, abs as spark_abs
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Pit_Intervals") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    pit = spark.read.parquet(base + "pit_race_only_2025.parquet")
    intervals = spark.read.parquet(base + "intervals_race_only_2025.parquet")
    sessions  = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers = spark.read.parquet(base + "drivers_second_half_2025.parquet")


    sessions_race = sessions.filter(
                        (col("meeting_key")  == 1270) &  # Singapore
                        (col("session_name") == "Race")
                    ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .join(sessions_race.select("session_key"), "session_key") \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    intervals_base = intervals \
                    .join(sessions_race, "session_key") \
                    .join(drivers_race, "driver_number")

    pit_filtered = pit \
                    .join(sessions_race, "session_key") \
                    .join(drivers_race, "driver_number")

    # chronologically ordered by date because intervals have no lap_number
    window_driver = Window \
                    .partitionBy("driver_number", "session_key") \
                    .orderBy("date")

    # lag/lead of 5 interval records (approximatelly 30s before/after)
    intervals_windowed = intervals_base \
                        .withColumn("gap_5_before", lag("gap_to_leader",  5).over(window_driver)) \
                        .withColumn("gap_5_after",  lead("gap_to_leader", 5).over(window_driver))

    # for each pit stop get interval records within 30s of pit date
    # one row per pit stop per driver
    result = pit_filtered.alias("p") \
                .join(
                    intervals_windowed.select(
                        "driver_number", "session_key", "date",
                        "gap_to_leader", "gap_5_before", "gap_5_after"
                    ).alias("i"),
                    (col("p.driver_number") == col("i.driver_number")) &
                    (col("p.session_key")   == col("i.session_key")) &
                    (spark_abs(col("i.date").cast("long") - col("p.date").cast("long")) < 30),
                    "left"
                ) \
                .groupBy(
                    col("p.driver_number"), col("p.full_name"), col("p.team_name"),
                    col("p.lap_number"),    col("p.stop_duration")
                ) \
                .agg(
                    avg("i.gap_to_leader").alias("gap_at_pit"),
                    avg("i.gap_5_before").alias("gap_before_pit"),
                    avg("i.gap_5_after").alias("gap_after_pit"),
                ) \
                .withColumn("gap_change_after_pit", col("gap_after_pit") - col("gap_before_pit")) \
                .withColumn("stop_duration", round(col("stop_duration"), 3)) \
                .withColumn("gap_at_pit", round(col("gap_at_pit"), 3)) \
                .withColumn("gap_before_pit", round(col("gap_before_pit"), 3)) \
                .withColumn("gap_after_pit", round(col("gap_after_pit"), 3)) \
                .withColumn("gap_change_after_pit", round(col("gap_change_after_pit"), 3)) \
                .withColumn("meeting_key", lit(1270)) \
                .orderBy("driver_number", "lap_number")

    result.write.format("mongodb") \
        .mode("overwrite") \
        .option("database",   mongo_db) \
        .option("collection", "f1_pit_intervals") \
        .save()

    print("Pit intervals Singapore computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()