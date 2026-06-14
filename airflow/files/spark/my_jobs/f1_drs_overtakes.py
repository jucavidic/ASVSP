import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, avg, lit, round, sum as spark_sum, row_number, max as spark_max, abs as spark_abs
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_DRS_Overtakes") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    overtakes = spark.read.parquet(base + "overtakes_race_only_2025.parquet")
    car_data = spark.read.parquet(base + "car_data_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    TARGET_TEAMS = ["Ferrari", "Red Bull Racing", "McLaren", "Mercedes"]

    sessions_filtered = sessions.filter(
                            (col("meeting_key")  == 1266) & # Budapest
                            (col("session_name") == "Race")
                        ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .filter(col("meeting_key") == 1266) \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    overtakes_base = overtakes \
                    .join(sessions_filtered, "session_key") \
                    .join(
                        drivers_race.select(
                            col("driver_number").alias("overtaking_driver_number"),
                            col("full_name").alias("overtaking_name"),
                            col("team_name").alias("overtaking_team"),
                        ),
                        "overtaking_driver_number"
                    ) \
                    .filter(col("overtaking_team").isin(TARGET_TEAMS)) \
                    .withColumn("overtake_ts", col("date").cast("long"))

    car_data_base = car_data \
                    .join(sessions_filtered, "session_key") \
                    .withColumn("car_ts", col("date").cast("long")) \
                    .select(
                        col("driver_number").alias("cd_driver"),
                        col("session_key").alias("cd_session"),
                        "car_ts",
                        col("drs").alias("car_drs"),
                        col("speed").alias("car_speed")
                    )

    df_joined = overtakes_base.alias("o").join(
                    car_data_base.alias("cd"),
                    (col("o.overtaking_driver_number") == col("cd.cd_driver")) &
                    (col("o.session_key") == col("cd.cd_session")),
                    "left"
                )

    '''

    # compute timestamp difference to find the closest telemetry data for the overtake
    diff_window = Window.partitionBy("o.overtaking_driver_number", "o.date") \
                         .orderBy(col("o.overtake_ts") - col("cd.car_ts"))

    df_closest = df_joined.withColumn("rn", row_number().over(diff_window)) \
                       .filter(col("rn") == 1) \
                       .drop("rn")

    # look at +- telemetry records before the overtake
    drs_window = Window.partitionBy("o.overtaking_driver_number", "o.date") \
                        .orderBy("cd.car_ts") \
                        .rowsBetween(-20, 20)

    df_with_drs = df_closest.withColumn(
                        "drs_in_window",
                        spark_max(when(col("car_drs").isin([10, 12, 14]), 1).otherwise(0)).over(drs_window)
                    ) \
                    .withColumn(
                        "is_drs_overtake",
                        when(col("drs_in_window") == 1, 1).otherwise(0)
                    )

    result = df_with_drs.groupBy(
                    "overtaking_driver_number", "overtaking_name", "overtaking_team"
                ).agg(
                    count("*").alias("total_overtakes"),
                    spark_sum("is_drs_overtake").alias("drs_overtakes"),
                    avg("car_speed").alias("avg_speed_at_overtake"),
                ) \
                .withColumn("drs_overtake_pct", round(when(col("total_overtakes") > 0, 
                           col("drs_overtakes") / col("total_overtakes") * 100).otherwise(0), 1)) \
                .withColumn("avg_speed_at_overtake", round("avg_speed_at_overtake", 3)) \
                .withColumn("meeting_key", lit(1266)) \
                .orderBy("overtaking_team", "overtaking_name")
    '''

    closest_window = Window.partitionBy("o.overtaking_driver_number", "o.date") \
                           .orderBy(spark_abs(col("o.overtake_ts") - col("cd.car_ts")))

    df_closest = df_joined.withColumn("rn", row_number().over(closest_window)) \
                          .filter(col("rn") == 1).drop("rn")

    # time window of around 4 seconds
    time_drs_window = Window.partitionBy("o.overtaking_driver_number", "o.date") \
                            .orderBy("cd.car_ts") \
                            .rowsBetween(-15, 15)   # around 4 seconds sampling rate

    df_with_drs = df_closest.withColumn(
                        "drs_in_window",
                        spark_max(when(col("car_drs").isin([10, 12, 14]), 1).otherwise(0)).over(time_drs_window)
                    ).withColumn(
                        "is_drs_overtake",
                        when(col("drs_in_window") == 1, 1).otherwise(0)
                    )

    result = df_with_drs.groupBy(
                    "overtaking_driver_number", "overtaking_name", "overtaking_team"
                ).agg(
                    count("*").alias("total_overtakes"),
                    spark_sum("is_drs_overtake").alias("drs_overtakes"),
                    avg("car_speed").alias("avg_speed_at_overtake")
                ) \
                .withColumn("drs_overtake_pct", round(when(col("total_overtakes") > 0, col("drs_overtakes") / col("total_overtakes") * 100).otherwise(0), 1)) \
                .withColumn("avg_speed_at_overtake", round("avg_speed_at_overtake", 3)) \
                .withColumn("meeting_key", lit(1266)) \
                .orderBy("overtaking_team", "overtaking_name")

    result.write.format("mongodb") \
                .mode("overwrite") \
                .option("database",   mongo_db) \
                .option("collection", "f1_drs_overtakes") \
                .save()

    print("DRS overtakes computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()
