from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def ingest_warm_data():
    """Execute the warm data ingestion script."""
    try:
        subprocess.run(["python", "data_ingestion/warm_paths/ingest_warm_data.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error during warm data ingestion: {e}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_warm_data',
    default_args=default_args,
    description='DAG to ingest warm data into Delta Lake',
    schedule_interval=timedelta(hours=6),  # Runs every 6 hours
    catchup=False,
)

run_warm_ingestion = PythonOperator(
    task_id='run_warm_data_ingestion',
    python_callable=ingest_warm_data,
    dag=dag,
)
