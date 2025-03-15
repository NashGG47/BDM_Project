from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_spark_job():
    """Run the Spark job to fetch and process transit incidents."""
    try:
        subprocess.run(["python", "data_ingestion/spark_jobs/process_transit_incidents.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error running Spark job: {e}")

# Define the DAG
dag = DAG(
    'transit_incidents_dag',
    description='Fetch and store transit incidents in Delta Lake',
    schedule_interval=timedelta(hours=1),  # Runs every hour
    start_date=datetime(2025, 3, 15),
    catchup=False,  # Do not run for past dates
)

# Define the task
run_task = PythonOperator(
    task_id='run_transit_job',
    python_callable=run_spark_job,
    dag=dag,
)
