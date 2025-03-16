from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

dag = DAG(
    'structured_data_ingestion',
    description='Automated ingestion of structured datasets (XML, CSV, SHP)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2025, 3, 15),
    catchup=False,
)

# Tasks for Warm Path (periodically updated data)
task_env = BashOperator(
    task_id="ingest_environmental_data",
    bash_command="python data_ingestion/warm_paths/environmental_ingestion.py",
    dag=dag,
)

task_pass = BashOperator(
    task_id="ingest_passenger_data",
    bash_command="python data_ingestion/warm_paths/passenger_ingestion.py",
    dag=dag,
)

# Task for Cold Path (static data)
task_shp = BashOperator(
    task_id="ingest_shapefile_data",
    bash_command="python data_ingestion/cold_paths/shapefile_ingestion.py",
    dag=dag,
)

# Define execution order: warm path tasks first, then cold path task
task_env >> task_pass >> task_shp
