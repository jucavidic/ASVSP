from pyspark.sql import SparkSession
import sys

def main():
    if len(sys.argv) < 3:
        print("ERROR: Missing arguments!")
        print("Usage: spark-submit transform_f1_data_heavy.py <raw_path> <transformed_path>")
        sys.exit(1)

    hdfs_raw_path = sys.argv[1].rstrip('/')
    hdfs_transformed_path = sys.argv[2].rstrip('/')

    spark = SparkSession.builder \
        .appName("F1_2025_Heavy_CSV_to_Parquet") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.network.timeout", "800s") \
        .config("spark.sql.shuffle.partitions", "16") \
        .config("spark.default.parallelism", "16") \
        .getOrCreate()

    drop_columns = {
        "car_data_race_only_2025.csv": ["brake", "n_gear"],
        "location_race_only_2025.csv": ["z"],
    }

    files = [
        "car_data_race_only_2025.csv",
        "location_race_only_2025.csv",
        "position_race_only_2025.csv",
    ]

    print(f"[HEAVY] Starting transform of {len(files)} files...")

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

    print("[HEAVY] Done.")
    spark.stop()

if __name__ == "__main__":
    main()