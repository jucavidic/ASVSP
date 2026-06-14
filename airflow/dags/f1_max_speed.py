from datetime import datetime
from airflow.sdk import dag, task
from utils import spark_submit, MONGO_HOST, HDFS_HOST


@dag(
    dag_id="f1_max_speed",
    description="Max speed in sector 3 for Ferrari vs top 3 competitors (Red Bull, McLaren, Mercedes) in Baku 2025",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def f1_max_speed():

    mongo_host = MONGO_HOST
    mongo_db = "f1_analysis"
    hdfs_base = HDFS_HOST

    @task.bash
    def run_query():
        return spark_submit(
            "my_jobs/f1_max_speed.py",
            packages=["org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"],
            args=[mongo_host, mongo_db, hdfs_base],
        )
    run_query()

f1_max_speed()