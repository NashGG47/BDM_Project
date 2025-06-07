from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime

dag_ids = ["ingest_emissions_data_dag", "ingest_environmental_indicators_dag", "ingest_gencat_incidents_dag", "ingest_renfe_incidents_dag", "ingest_google_maps_images_dag" , "ingest_passenger_volume_dag", "ingest_serapi_images_dag","ingest_twitter_data_dag","ingest_admin_shapefiles_dag" , "emissions_to_influxdb"]       


with DAG("master_dag", start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:
    for dag_id in dag_ids:
        TriggerDagRunOperator(
            task_id=f"trigger_{dag_id}",
            trigger_dag_id=dag_id,
        )
