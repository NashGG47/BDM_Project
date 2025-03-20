from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_ingestion_job():
    """Run the ingestion job to fetch and store SERAPI images data."""
    try:
        subprocess.run(["python", "cold_paths/ingest_cold_serapi_images.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ingestion job: {e}")

dag = DAG(
    'serapi_dag',
    description='Fetch and store SERAPI image data in Delta Lake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

ingest_task = PythonOperator(
    task_id='run_ingestion_job',
    python_callable=run_ingestion_job,
    dag=dag,
)

