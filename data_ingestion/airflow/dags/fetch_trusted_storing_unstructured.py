from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging

def store_trusted_zone():
    """Run the trusted zone storage script for Bluesky posts."""
    try:
        result = subprocess.run(
            ["python3", "trusted_zone/storing_unstructured.py"],
            check=True,
            capture_output=True,
            text=True
        )
        logging.info("Trusted zone storage output:\n%s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Error in trusted zone storage: %s", e.stderr)
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 15),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id='store_bluesky_trusted_dag',
    description='Store cleaned Bluesky posts into trusted zone',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["trusted_zone", "bluesky", "minio"],
)

run_storage = PythonOperator(
    task_id='store_to_trusted_zone',
    python_callable=store_trusted_zone,
    dag=dag,
)
