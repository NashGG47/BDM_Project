from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess


def run_consumer_job():
    print("Starting Bluesky streaming consumer...")
    try:
        subprocess.run(
            ["python3", "data_ingestion/hot_paths/consumer/consume_bluesky.py"],
            check=True
        )
    except subprocess.TimeoutExpired:
        print("Consumer ended due to 30-minute timeout (OK)")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Consumer failed: {e}")

def run_producer_job():
    print("Starting Bluesky streaming producer...")
    try:
        subprocess.run(
            ["python3", "data_ingestion/hot_paths/producer/bluesky_producer.py"],
            check=True
        )
    except subprocess.TimeoutExpired:
        print("Producer ended due to 30-minute timeout (OK)")
    except subprocess.CalledProcessError as e:
        print(f"[ERROR] Producer failed: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'start_date': datetime(2025, 3, 15),
}

dag = DAG(
    'ingest_bluesky_data_streaming_dag',
    default_args=default_args,
    description='[STREAMING] Bluesky producer and consumer with 30-minute timeout',
    schedule_interval=None,
    catchup=False,
)

run_producer_task = PythonOperator(
    task_id='run_bluesky_producer_streaming',
    python_callable=run_producer_job,
    dag=dag,
)

run_consumer_task = PythonOperator(
    task_id='run_bluesky_consumer_streaming',
    python_callable=run_consumer_job,
    dag=dag,
)

[run_producer_task, run_consumer_task]
