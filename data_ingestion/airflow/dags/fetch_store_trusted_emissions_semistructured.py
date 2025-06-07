from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from pathlib import Path
import subprocess

PROJECT_ROOT = Path(__file__).resolve().parent.parent

def run_emissions_script():
    script = PROJECT_ROOT / "trusted_zone/storing_co2_semistructured.py"
    subprocess.run(["python", str(script)], check=True)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025,3,15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag= DAG(
    'emissions_to_influxdb', 
    schedule=timedelta(minutes=1), 
    default_args=default_args, 
    catchup=False
)

run_emissions_script_task = PythonOperator(
        task_id='run_emissions_script',
        python_callable=run_emissions_script,
        dag = dag,
)

run_emissions_script_task