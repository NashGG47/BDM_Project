from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

def ingest_google_maps_images():
    """Execute the cold path ingestion script for Google Maps routes."""
    try:
        result = subprocess.run(
            ["python3", "data_ingestion/cold_paths/ingest_cold_google_maps_images.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Ingestion completed. Output:\n%s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Error during Google Maps ingestion: %s", e.stderr)
        raise

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='ingest_google_maps_images_dag',
    default_args=default_args,
    description='Cold path DAG to ingest Google Maps route images into Delta Lake',
    schedule=timedelta(days=1),  # Runs daily
    catchup=False,
    tags=["cold_path", "google_maps", "delta_lake"],
) 

run_google_maps_ingestion = PythonOperator(
    task_id='run_google_maps_ingestion',
    python_callable=ingest_google_maps_images,
    dag=dag
)
