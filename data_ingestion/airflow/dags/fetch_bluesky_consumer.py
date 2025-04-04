from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_consumer():
    print("Starting Bluesky streaming consumer...")
    try:
        subprocess.run(
            ["python3", "/home/nashly/BDM_Project/data_ingestion/hot_paths/consumer/consume_bluesky.py"],
            timeout=900,  # 30 minutes
            check=True
        )
        print("Consumer finished successfully.")
    except subprocess.TimeoutExpired:
        print("Consumer stopped after timeout (30 minutes).")
    except subprocess.CalledProcessError as e:
        print(f"Consumer failed: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 15),
    'retries': 0,
}

dag = DAG(
    'bluesky_consumer_dag',
    description='Run Bluesky consumer script for streaming',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

run_consumer_task = PythonOperator(
    task_id='run_bluesky_consumer',
    python_callable=run_consumer,
    dag=dag,
)
