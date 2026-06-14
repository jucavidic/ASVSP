from datetime import datetime
from airflow.sdk import dag, task
from utils import spark_submit, HDFS_HOST
import os

@dag(
    dag_id="transform_raw_f1_data",
    description="Transform F1 CSVs to Parquet — heavy and light jobs",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def transform_raw_f1_data():

    hdfs_raw_path = f"{HDFS_HOST}/raw/f1_2025"
    hdfs_transformed_path = f"{HDFS_HOST}/transformed/f1_2025"

    @task.bash
    def run_heavy_job():
        return spark_submit(
            "my_jobs/transform_f1_data_heavy.py",
            args=[hdfs_raw_path, hdfs_transformed_path],
        )

    @task.bash
    def run_light_job():
        return spark_submit(
            "my_jobs/transform_f1_data_light.py",
            args=[hdfs_raw_path, hdfs_transformed_path],
        )

    run_heavy_job()
    run_light_job()


transform_raw_f1_data()