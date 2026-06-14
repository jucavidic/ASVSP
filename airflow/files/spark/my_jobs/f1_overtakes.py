import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, when, lag, lit, coalesce, max as spark_max
)
from pyspark.sql.window import Window


def main():
    mongo_host = sys.argv[1]
    mongo_db   = sys.argv[2]
    hdfs_base  = sys.argv[3]

    spark = SparkSession.builder \
        .appName("F1_Overtakes_and_Positions_Zandvoort") \
        .config("spark.mongodb.write.connection.uri", mongo_host) \
        .getOrCreate()

    base = hdfs_base + "/transformed/f1_2025/"

    overtakes = spark.read.parquet(base + "overtakes_race_only_2025.parquet")
    position  = spark.read.parquet(base + "position_race_only_2025.parquet")
    sessions  = spark.read.parquet(base + "sessions_second_half_2025.parquet")
    drivers   = spark.read.parquet(base + "drivers_second_half_2025.parquet")

    MEETING_KEY     = 1267
    FERRARI_DRIVERS = [16, 44]

    sessions_race = sessions.filter(
                        (col("meeting_key")  == MEETING_KEY) &
                        (col("session_name") == "Race")
                    ).select("session_key", "meeting_key")

    drivers_race = drivers \
                    .join(sessions_race.select("session_key"), "session_key") \
                    .select("driver_number", "full_name", "team_name") \
                    .dropDuplicates(["driver_number"])

    # part 1 - overtakes
    overtakes_filtered = overtakes \
                        .join(sessions_race, "session_key") \
                        .filter(col("overtaking_driver_number").isin(FERRARI_DRIVERS)) \
                        .join(
                            drivers_race.select("driver_number", "full_name"),
                            overtakes["overtaking_driver_number"] == drivers_race["driver_number"],
                            "left"
                        )

    # chronologically ordered overtakes
    window_cumulative = Window \
                        .partitionBy("overtaking_driver_number", "session_key") \
                        .orderBy("date") \
                        .rowsBetween(Window.unboundedPreceding, 0)

    overtakes_with_cum = overtakes_filtered \
                        .withColumn("cumulative_overtakes", spark_sum(lit(1)).over(window_cumulative)) \
                        .select(
                            "overtaking_driver_number", "full_name", "session_key",
                            "date", "cumulative_overtakes"
                        ) \
                        .withColumn("meeting_key", lit(MEETING_KEY))

    result_overtakes = overtakes_with_cum.groupBy(
                                "overtaking_driver_number", "full_name", "session_key"
                            ).agg(
                                spark_max("cumulative_overtakes").alias("total_overtakes")
                            ).withColumn("meeting_key", lit(MEETING_KEY))

    # part 2 - position gain/loss
    position_ferrari = position \
                        .join(sessions_race, "session_key") \
                        .join(drivers_race.select("driver_number", "full_name"), "driver_number") \
                        .filter(col("driver_number").isin(FERRARI_DRIVERS))

    # chronologically ordered positions
    window_position = Window \
                        .partitionBy("driver_number", "session_key") \
                        .orderBy("date")

    position_with_change = position_ferrari \
                            .withColumn("prev_position", lag("position", 1).over(window_position)) \
                            .withColumn(
                                "position_change",
                                coalesce(col("prev_position") - col("position"), lit(0))
                            )

    result_position = position_with_change.groupBy("driver_number", "full_name") \
                        .agg(
                            spark_sum(when(col("position_change") > 0, col("position_change")).otherwise(0))
                                .alias("total_positions_gained"),
                            spark_sum(when(col("position_change") < 0, col("position_change")).otherwise(0))
                                .alias("total_positions_lost"),
                        ) \
                        .withColumn("meeting_key", lit(MEETING_KEY))


    result_overtakes.write.format("mongodb").mode("overwrite") \
                .option("database", mongo_db) \
                .option("collection", "f1_overtakes") \
                .save()

    result_position.write.format("mongodb").mode("overwrite") \
                .option("database", mongo_db) \
                .option("collection", "f1_position_change") \
                .save()

    print("Overtakes and positions computed and saved!")
    spark.stop()


if __name__ == "__main__":
    main()