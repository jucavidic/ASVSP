import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, corr, avg, count, when, lit, round
from pyspark.sql.functions import min as spark_min, max as spark_max
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_DRS_Correlation") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    car_data = spark.read.parquet(base + "car_data_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers  = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    sessions_race = sessions.filter(
                        (col("meeting_key")  == 1265) &
                        (col("session_name") == "Race")
                    ).select("session_key", "meeting_key")

    drivers_race = drivers \
                   .join(sessions_race.select("session_key"), "session_key") \
                   .select("driver_number", "full_name", "team_name") \
                   .dropDuplicates(["driver_number"])
  
    # note: eliminate formation lap, pit lane or safety car
    df_filtered = car_data \
                  .join(sessions_race, "session_key") \
                  .join(drivers_race, "driver_number") \
                  .filter(col("team_name") == "Ferrari") \
                  .filter(col("speed") > 0) \
                  .dropDuplicates(["driver_number", "date"])  

    #keep only clearly defined values
    df_filtered = df_filtered.filter(col("drs").isin([0, 1, 8, 10, 12, 14]))

    df_filtered = df_filtered.withColumn(
        "drs_on",
        when(col("drs").isin([10, 12, 14]), 1)
        .when(col("drs") == 8, 0.5)
        .otherwise(0)
    )

    window_driver = Window.partitionBy("driver_number").orderBy("date").rowsBetween(-5, 5)

    df_analysis = df_filtered.withColumn(
        "speed_moving_avg",
        avg("speed").over(window_driver)
    )

    result = df_analysis.groupBy("driver_number", "full_name") \
        .agg(
            corr("drs_on", "speed").alias("drs_speed_correlation"),
            avg("speed").alias("avg_speed"),
            avg("speed_moving_avg").alias("avg_speed_moving"),
            count(when(col("drs_on") > 0, 1)).alias("drs_activations"),
            avg(when(col("drs_on") > 0,  col("speed"))).alias("avg_speed_when_drs_on"),
            avg(when(col("drs_on") == 0, col("speed"))).alias("avg_speed_when_drs_off"),
            count("*").alias("total_samples"),
        ) \
        .withColumn("meeting_key", lit(1265)) \
        .orderBy(col("drs_speed_correlation").desc())

    print(">> DRS and speed correlation for Ferrari drivers in SPA 2025 <<")
    result.select(
        "full_name",
        round("drs_speed_correlation",  4).alias("correlation"),
        round("avg_speed",              2).alias("avg_speed_kmh"),
        round("avg_speed_when_drs_on",  2).alias("avg_speed_drs_on"),
        round("avg_speed_when_drs_off", 2).alias("avg_speed_drs_off"),
        "drs_activations",
        "total_samples",
    ).show(truncate=False)

    result.write.format("mongodb") \
        .mode("overwrite") \
        .option("database",   mongo_db) \
        .option("collection", "f1_drs_correlation") \
        .save()

    print("DRS correlation computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()