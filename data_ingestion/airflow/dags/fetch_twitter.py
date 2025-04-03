from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

def ingest_twitter_data():
    """Run the cold path ingestion script for Twitter data."""
    try:
        result = subprocess.run(
            ["python3", "data_ingestion/cold_paths/ingest_cold_twitter_data.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Twitter ingestion completed:\n%s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Error during Twitter ingestion: %s", e.stderr)
        raise

# Default args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    dag_id='ingest_twitter_data_dag',
    default_args=default_args,
    description='Cold path DAG to ingest Twitter posts and images into Delta Lake',
    schedule_interval=timedelta(days=1),  # daily run
    catchup=False,
    tags=["cold_path", "twitter", "delta_lake"],
)

run_twitter_ingestion = PythonOperator(
    task_id='run_twitter_ingestion',
    python_callable=ingest_twitter_data,
    dag=dag,
)
