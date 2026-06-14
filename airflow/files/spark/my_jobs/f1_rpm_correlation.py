import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, corr, lit, round, floor, row_number, dense_rank
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
            .appName("F1_RPM_Correlation") \
            .config("spark.mongodb.write.connection.uri", mongo_host) \
            .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    car_data = spark.read.parquet(base + "car_data_race_only_2025.parquet")
    location = spark.read.parquet(base + "location_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers  = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    sessions_filtered = sessions.filter(
                            (col("meeting_key")  == 1266) &   # Budapest
                            (col("session_name") == "Race")
                        ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .join(sessions_filtered.select("session_key"), "session_key") \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    # round dates per second to lower the number of relevant measurings
    car_rounded = car_data \
                .join(sessions_filtered, "session_key") \
                .join(drivers_race.select("driver_number", "full_name", "team_name"), "driver_number") \
                .filter(col("team_name") == "Ferrari") \
                .withColumn("date_sec", floor(col("date").cast("long")))

    loc_rounded = location \
                .join(sessions_filtered.select("session_key"), "session_key") \
                .withColumn("date_sec", floor(col("date").cast("long")))

    df_joined = car_rounded.join(
                    loc_rounded.select("session_key", "driver_number", "date_sec", "x", "y"),
                    ["session_key", "driver_number", "date_sec"],
                    "left"
                )

    # divide track into zones of 100 meters
    df_binned = df_joined.withColumn("x_bin", (floor(col("x") / 100) * 100).cast("int"))

    window_smooth = Window \
                    .partitionBy("driver_number", "session_key") \
                    .orderBy("date") \
                    .rowsBetween(-8, 8)

    df_smoothed = df_binned \
                    .withColumn("smoothed_speed", avg("speed").over(window_smooth)) \
                    .withColumn("smoothed_rpm",   avg("rpm").over(window_smooth))

    # throttle -> percentage of maximum engine power being used
    result = df_smoothed.groupBy("driver_number", "full_name", "x_bin") \
                .agg(
                    avg("smoothed_speed").alias("avg_speed"),
                    avg("smoothed_rpm").alias("avg_rpm"),
                    corr("rpm", "speed").alias("rpm_speed_corr"),
                    avg("throttle").alias("avg_throttle"),
                ) \
                .withColumn("meeting_key", lit(1266)) \
                .orderBy("driver_number", "x_bin")

    # get zone numbers
    window_zone = Window.partitionBy("driver_number").orderBy("x_bin")

    result = result \
            .withColumn("zone_index", dense_rank().over(window_zone)) \
            .drop("x_bin") \
            .orderBy("driver_number", "zone_index")

    result.write.format("mongodb") \
                .mode("overwrite") \
                .option("database",   mongo_db) \
                .option("collection", "f1_rpm_correlation") \
                .save()

    print("RPM and speed correlation computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()
