from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_consumer_job():
    """Run the Consumer job to fetch and process transit incidents from Kafka."""
    try:
        subprocess.run(["python", "data_ingestion/hot_paths/consumer/consume_emissions_dta.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Consumer job: {e}")

def run_producer_job():
    """Run the Producer job to send transit incidents to Kafka."""
    try:
        subprocess.run(["python", "data_ingestion/hot_paths/producer/process_emissions_data.py"], check=True)
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
    "emissions_data_dag",
    description="Fetch, process, and store transit incidents using Kafka and Delta Lake",
    schedule_interval=timedelta(hours=1),
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

