from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

def ingest_serapi_images():
    """Run the cold path ingestion script for SERAPI images."""
    try:
        result = subprocess.run(
            ["python3", "data_ingestion/cold_paths/ingest_cold_serapi_images.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Ingestion script output:\n%s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Error running ingestion job: %s", e.stderr)
        raise

# DAG default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    dag_id='ingest_serapi_images_dag',
    description='Fetch and store SERAPI image data in Delta Lake',
    default_args=default_args,
    schedule=timedelta(days=1),
    start_date=datetime(2025, 3, 15),
    catchup=False,
    tags=["cold_path", "serapi", "delta_lake"],
)
   
run_serapi_ingestion = PythonOperator(
    task_id='run_serapi_ingestion',
    python_callable=ingest_serapi_images,
    dag=dag,
)
