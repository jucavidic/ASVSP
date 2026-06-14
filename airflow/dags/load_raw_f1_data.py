from datetime import datetime
from airflow.sdk import dag, task
from airflow.providers.apache.hdfs.hooks.webhdfs import WebHDFSHook
import os

@dag(
    dag_id="load_raw_f1_data",
    description="Load local CSVs from files/data to HDFS raw zone",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        'owner': 'airflow',
        'retries': 2,
    }
)
def load_raw_f1_data():
    
    # path configuration
    airflow_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
    local_data_path = f"{airflow_home}/files/data"  
    hdfs_raw_path = "/raw/f1_2025/"

    @task
    def load_files_to_hdfs():
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")

        try:
            files = [f for f in os.listdir(local_data_path) if f.endswith('.csv')]
        except FileNotFoundError:
            raise FileNotFoundError(f"Local directory not found: {local_data_path}")

        if not files:
            print(f"No CSV files found in {local_data_path}")
            return hdfs_raw_path

        print(f"Found {len(files)} CSV files. Starting upload...")
        
        for file_name in files:
            local_file = os.path.join(local_data_path, file_name)
            hdfs_dest = f"{hdfs_raw_path}{file_name}"
            
            print(f"Uploading {file_name} to HDFS...")
            hdfs_hook.load_file(source=local_file, destination=hdfs_dest, overwrite=True)

        print(f"All files uploaded successfully!")
        
        return hdfs_raw_path

    @task
    def verify_hdfs_files(hdfs_path: str):
        hdfs_hook = WebHDFSHook(webhdfs_conn_id="HDFS_CONNECTION")
        
        print(f"Verifying files in HDFS path: {hdfs_path}")
        
        try:
            conn = hdfs_hook.get_conn()
            files_in_hdfs = conn.list(hdfs_path)
            
            csv_files = [f for f in files_in_hdfs if f.endswith('.csv')]
            
            print(f"Found {len(csv_files)} CSV files on HDFS:")
            for f in sorted(csv_files):
                print(f" - {f}")
            
            if len(csv_files) == 0:
                raise ValueError("No CSV files found in HDFS!")
            
            return f"Verification successful: {len(csv_files)} files"
        
        except Exception as e:
            print(f"Verification failed: {e}")
            raise

    hdfs_path = load_files_to_hdfs()
    verify_hdfs_files(hdfs_path)


load_raw_f1_data()