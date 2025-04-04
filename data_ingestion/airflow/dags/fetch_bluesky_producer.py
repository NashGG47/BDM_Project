from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_producer():
    print("Starting Bluesky streaming producer...")
    try:
        subprocess.run(
            ["python3", "/home/nashly/BDM_Project/data_ingestion/hot_paths/producer/bluesky_producer.py"],
            timeout=900,  # 30 minutes
            check=True
        )
        print("Producer finished successfully.")
    except subprocess.TimeoutExpired:
        print("Producer stopped after timeout (30 minutes).")
    except subprocess.CalledProcessError as e:
        print(f"Producer failed: {e}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 15),
    'retries': 0,
}

dag = DAG(
    'bluesky_producer_dag',
    description='Run Bluesky producer script for streaming',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

run_producer_task = PythonOperator(
    task_id='run_bluesky_producer',
    python_callable=run_producer,
    dag=dag,
)
