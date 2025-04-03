from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def run_passenger_volume_ingestion():
    try:
        subprocess.run(
            ["python", "data_ingestion/warm_paths/ingest_passenger_volume.py"],
            check=True
        )
    except subprocess.CalledProcessError as e:
        print(f"Error during passenger volume ingestion: {e}")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_passenger_volume_dag',
    default_args=default_args,
    description='DAG to ingest warm passenger volume data into Delta Lake',
    schedule_interval=timedelta(days=1),  # fetch data daily
    catchup=False,
)

run_task = PythonOperator(
    task_id='run_passenger_volume_ingestion',
    python_callable=run_passenger_volume_ingestion,
    dag=dag,
)
