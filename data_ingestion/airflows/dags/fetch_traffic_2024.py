from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_ingestion_job():
    """Run the ingestion job to fetch and store 2025 traffic data."""
    try:
        subprocess.run(["python", "cold_paths/ingest_cold_traffic_data.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running ingestion job: {e}")

dag = DAG(
    'traffic_2025_dag',
    description='Fetch and store 2025 traffic data in Delta Lake',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

ingest_task = PythonOperator(
    task_id='run_ingestion_job',
    python_callable=run_ingestion_job,
    dag=dag,
)
