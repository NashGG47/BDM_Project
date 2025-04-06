from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

# Get the absolute path to the project root (adjust as needed)
PROJECT_ROOT = Path(__file__).resolve().parent.parent  

def run_consumer_job():
    """Run the Consumer job to fetch and process transit incidents from Kafka."""
    try:
        consumer_script = PROJECT_ROOT / "data_ingestion/hot_paths/consumer/consume_gencat_incident.py"
        subprocess.run(["python", str(consumer_script)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Consumer job: {e}")

def run_producer_job():
    """Run the Producer job to send transit incidents to Kafka."""
    try:
        producer_script = PROJECT_ROOT / "data_ingestion/hot_paths/producer/process_gencat_incidents.py"
        subprocess.run(["python", str(producer_script)], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Producer job: {e}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 15),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ingest_gencat_incidents_dag",
    description="Fetch, process, and store transit incidents using Kafka and Delta Lake",
    schedule=timedelta(hours=1), 
    catchup=False,
    default_args=default_args,
)

run_producer_task = PythonOperator(
    task_id="run_producer_job",
    python_callable=run_producer_job,
    dag=dag,
)

run_consumer_task = PythonOperator(
    task_id="run_consumer_job",
    python_callable=run_consumer_job,
    dag=dag,
)

run_producer_task >> run_consumer_task
