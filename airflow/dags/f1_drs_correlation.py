from datetime import datetime
from airflow.sdk import dag, task
from utils import spark_submit, MONGO_HOST, HDFS_HOST

@dag(
    dag_id="f1_drs_correlation",
    description="DRS usage vs speed correlation for Ferrari in SPA 2025",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def f1_drs_correlation():

    mongo_host = MONGO_HOST
    mongo_db = "f1_analysis"
    hdfs_base = HDFS_HOST

    @task.bash
    def run_query():
        return spark_submit(
            "my_jobs/f1_drs_correlation.py",
            packages=["org.mongodb.spark:mongo-spark-connector_2.13:10.6.0"],
            args=[mongo_host, mongo_db, hdfs_base]
        )

    run_query()

f1_drs_correlation()