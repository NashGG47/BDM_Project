from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess
import logging


def run_traffic_job():
    logging.info("Running trusted_zone/storing_transit_semistructured.py...")

    try:
        result = subprocess.run(
            ["python", "trusted_zone/storing_transit_semistructured.py"],
            capture_output=True,
            text=True,
            check=True
        )
        logging.info("Script completed successfully.")
        logging.info(f"stdout: {result.stdout}")
        logging.info(f"stderr: {result.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error("Script failed!")
        logging.error(f"Return code: {e.returncode}")
        logging.error(f"stdout: {e.stdout}")
        logging.error(f"stderr: {e.stderr}")
        raise

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ingest_traffic_to_influxdb_dag",
    description="Process and store traffic incidents data into InfluxDB",
    schedule=timedelta(minutes=30),  # or timedelta(hours=1), depending on your use case
    catchup=False,
    default_args=default_args,
)

run_traffic_task = PythonOperator(
    task_id="run_traffic_job",
    python_callable=run_traffic_job,
    dag=dag,
)

run_traffic_task
