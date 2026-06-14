import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, lag, first, lit, round as spark_round, when, count, coalesce
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Gap_To_Leader") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    # note - intervals measured every few seconds
    intervals = spark.read.parquet(base + "intervals_race_only_2025.parquet")
    sessions  = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers   = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    MEETING_KEY  = 1274
    TARGET_TEAMS = ["Ferrari", "Red Bull Racing", "McLaren", "Mercedes"]

    sessions_race = sessions.filter(
                        (col("meeting_key")  == MEETING_KEY) &
                        (col("session_name") == "Race")
                    ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .join(sessions_race.select("session_key"), "session_key") \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    intervals_filtered = intervals \
                            .join(sessions_race, "session_key") \
                            .join(drivers_race.select("driver_number", "full_name", "team_name"), "driver_number") \
                            .filter(col("team_name").isin(TARGET_TEAMS)) \
                            .dropDuplicates(["driver_number", "date"]) \
                            .withColumn("gap_to_leader", coalesce(col("gap_to_leader"), lit(0.0)))

    window_rolling10 = Window \
                        .partitionBy("driver_number", "session_key") \
                        .orderBy("date") \
                        .rowsBetween(-10, 0)

    window_driver = Window \
                    .partitionBy("driver_number", "session_key") \
                    .orderBy("date")

    window_full = Window \
                    .partitionBy("driver_number", "session_key") \
                    .orderBy("date") \
                    .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df_analysis = intervals_filtered \
                    .withColumn("avg_gap_last10", avg("gap_to_leader").over(window_rolling10)) \
                    .withColumn("gap_5_ago",      lag("gap_to_leader", 5).over(window_driver)) \
                    .withColumn("gap_at_start",   first("gap_to_leader").over(window_full))

    # gap timeline
    df_analysis.select(
                        "driver_number", "full_name", "team_name", "date",
                        spark_round(col("gap_to_leader"), 3).alias("gap_to_leader"),
                        spark_round(col("avg_gap_last10"), 3).alias("avg_gap_last10"),
                        spark_round(col("gap_5_ago"),      3).alias("gap_5_ago")
                      ) \
                      .withColumn("meeting_key", lit(MEETING_KEY)) \
                      .write.format("mongodb").mode("overwrite") \
                      .option("database", mongo_db) \
                      .option("collection", "f1_gap_timeline") \
                      .save()

    # get gap results by driver
    result = df_analysis.groupBy("driver_number", "full_name", "team_name") \
                .agg(
                    avg("gap_to_leader").alias("avg_gap_overall"),
                    avg("avg_gap_last10").alias("avg_rolling10_gap"),
                    avg("gap_at_start").alias("gap_race_start"),
                    # gap_to_leader is 0.0 for race leader
                    count(when(col("gap_to_leader") == 0.0, lit(1))).alias("records_as_leader"),
                ) \
                .withColumn("avg_gap_overall",   spark_round(col("avg_gap_overall"), 3)) \
                .withColumn("avg_rolling10_gap", spark_round(col("avg_rolling10_gap"), 3)) \
                .withColumn("gap_race_start",    spark_round(col("gap_race_start"), 3)) \
                .withColumn("meeting_key", lit(MEETING_KEY)) \
                .orderBy("avg_gap_overall")

    result.write.format("mongodb").mode("overwrite") \
                .option("database", mongo_db) \
                .option("collection", "f1_gap_to_leader") \
                .save()

    print("Gap to leader computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()