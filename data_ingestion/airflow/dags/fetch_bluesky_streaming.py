from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 15),
    'retries': 0,
}

dag = DAG(
    'launch_bluesky_streaming_dag',
    description='Trigger both Bluesky producer and consumer DAGs in parallel',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
)

trigger_producer = TriggerDagRunOperator(
    task_id='trigger_producer_dag',
    trigger_dag_id='bluesky_producer_dag',
    dag=dag,
)

trigger_consumer = TriggerDagRunOperator(
    task_id='trigger_consumer_dag',
    trigger_dag_id='bluesky_consumer_dag',
    dag=dag,
)

# Run both tasks at the same time
[trigger_producer, trigger_consumer]
