import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, avg, lit, round, min as spark_min, struct


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Max_Speed_Sector3") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    laps     = spark.read.parquet(base + "laps_race_only_2025.parquet")
    sessions = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers  = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    TARGET_TEAMS = ["Ferrari", "Red Bull Racing", "McLaren", "Mercedes"]

    sessions_filtered = sessions.filter(
                            (col("meeting_key")  == 1269) & #Baku
                            (col("session_name") == "Race")
                        ).select("session_key", "meeting_key")

    laps_filtered = laps \
                    .join(sessions_filtered, "session_key") \
                    .join(drivers.select("driver_number", "full_name", "team_name"), "driver_number") \
                    .filter(col("team_name").isin(TARGET_TEAMS))

    
    result = laps_filtered.groupBy("driver_number", "full_name", "team_name") \
                .agg(
                    spark_max("st_speed").alias("best_speed_trap"),  # trap is specific point of trach with highest speed records
                    avg("st_speed").alias("avg_speed_trap"),
                    spark_min("duration_sector_3").alias("best_s3_time"),
                    avg("duration_sector_3").alias("avg_s3_time"),
                    spark_max("lap_number").alias("laps_completed"),
                    spark_max(struct(col("st_speed"), col("lap_number"))) # lap when best trap speed recorder
                        .alias("best_trap_struct"),
                    spark_min(struct(col("duration_sector_3"), col("lap_number"))) # lap when best sector 3 time recorder
                        .alias("best_s3_struct"),
                ) \
                .withColumn("lap_best_speed_trap", col("best_trap_struct.lap_number")) \
                .withColumn("lap_best_s3_time",    col("best_s3_struct.lap_number")) \
                .withColumn("avg_speed_trap", round(col("avg_speed_trap"), 2)) \
                .withColumn("avg_s3_time",    round(col("avg_s3_time"), 3)) \
                .drop("best_trap_struct", "best_s3_struct") \
                .withColumn("meeting_key", lit(1269)) \
                .orderBy(col("best_speed_trap").desc())

    result.write.format("mongodb") \
        .mode("overwrite") \
        .option("database",   mongo_db) \
        .option("collection", "f1_max_speed") \
        .save()

    print("Max speed computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()
