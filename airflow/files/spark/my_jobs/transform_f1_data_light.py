from pyspark.sql import SparkSession
import sys

def main():
    if len(sys.argv) < 3:
        print("ERROR: Missing arguments!")
        print("Usage: spark-submit transform_f1_data_light.py <raw_path> <transformed_path>")
        sys.exit(1)

    hdfs_raw_path = sys.argv[1].rstrip('/')
    hdfs_transformed_path = sys.argv[2].rstrip('/')

    spark = SparkSession.builder \
        .appName("F1_2025_Light_CSV_to_Parquet") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.network.timeout", "800s") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.default.parallelism", "8") \
        .getOrCreate()

    drop_columns = {
        "laps_race_only_2025.csv": ["i1_speed", "i2_speed", "segments_sector_1", 
                                    "segments_sector_2", "segments_sector_3"],
        "sessions_second_half_2025.csv": ["circuit_key", "circuit_short_name", "country_code",
                                          "country_key", "gmt_offset", "date_end",
                                          "meeting_official_name", "is_cancelled", "year"],
        "drivers_second_half_2025.csv": ["broadcast_name", "headshot_url",
                                         "name_acronym", "team_colour"],
        "meetings_second_half_2025.csv": ["meeting_official_name", "circuit_key", "circuit_image",
                                          "circuit_info_url", "circuit_type", "country_code",
                                          "country_flag", "country_key", "gmt_offset",
                                          "date_start", "date_end", "is_cancelled"],
    }

    files = [
        "laps_race_only_2025.csv",
        "sessions_second_half_2025.csv",
        "drivers_second_half_2025.csv",
        "meetings_second_half_2025.csv",
        "intervals_race_only_2025.csv",
        "overtakes_race_only_2025.csv",
        "pit_race_only_2025.csv",
        "race_sessions_2025.csv",
        "stints_race_only_2025.csv",
    ]

    print(f"[LIGHT] Starting transform of {len(files)} files...")

    for csv_file in files:
        input_path = f"{hdfs_raw_path}/{csv_file}"
        output_path = f"{hdfs_transformed_path}/{csv_file.replace('.csv', '.parquet')}"

        print(f"Processing: {csv_file}")
        df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

        if csv_file in drop_columns:
            cols_to_drop = [c for c in drop_columns[csv_file] if c in df.columns]
            if cols_to_drop:
                df = df.drop(*cols_to_drop)
                print(f"  Dropped: {cols_to_drop}")

        for old_col in df.columns:
            new_col = old_col.lower().replace(" ", "_").replace("-", "_").replace(".", "_")
            if new_col != old_col:
                df = df.withColumnRenamed(old_col, new_col)

        df.write.mode("overwrite").parquet(output_path)
        print(f"  Saved -> {output_path} ({len(df.columns)} columns)")

    print("[LIGHT] Done.")
    spark.stop()

if __name__ == "__main__":
    main()