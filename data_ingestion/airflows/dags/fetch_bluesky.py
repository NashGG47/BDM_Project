from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_consumer_job():
    """Run the Consumer job to fetch and process bluesky from Kafka."""
    try:
        subprocess.run(["python", "hotpaths/consumer/bluesky_producer.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Consumer job: {e}")

def run_producer_job():
    """Run the Producer job to send bluesky to Kafka."""
    try:
        subprocess.run(["python", "producer/bluesky_producer.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Producer job: {e}")


dag = DAG(
    'bluesky_dag',
    description='Fetch, process, and store bluesky using Kafka and Delta Lake',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 3, 15),
    catchup=False, 
)

run_producer_task = PythonOperator(
    task_id='run_producer_job',
    python_callable=run_producer_job,
    dag=dag,
)

run_consumer_task = PythonOperator(
    task_id='run_consumer_job',
    python_callable=run_consumer_job,
    dag=dag,
)
run_producer_task >> run_consumer_task 