from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

def ingest_cold_data():
    """Execute the cold data ingestion script."""
    try:
        subprocess.run(["python", "data_ingestion/cold_paths/ingest_admin_shapefiles.py"], check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error during cold data ingestion: {e}")

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 18),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_admin_shapefiles_dag',
    default_args=default_args,
    description='DAG to ingest cold data into Delta Lake',
    schedule=timedelta(days=3),  # Runs each 3 days
    catchup=False,
)

run_cold_ingestion = PythonOperator(
    task_id='run_cold_data_ingestion',
    python_callable=ingest_cold_data,
    dag=dag,
)
