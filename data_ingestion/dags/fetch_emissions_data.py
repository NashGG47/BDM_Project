from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kafka import KafkaConsumer
import json
from delta import *
from pyspark.sql import SparkSession
import time

# Delta Lake configuration
DELTA_TABLE_PATH = "storage/delta/raw/emissions_data"  # Update this path

# Initialize Spark Session with Delta Lake support
spark = SparkSession.builder \
    .appName("KafkaToDelta") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Function to consume Kafka data and save to Delta Lake
def consume_and_save_to_delta():
    consumer = KafkaConsumer(
        'emissions_data',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )

    for message in consumer:
        data = message.value

        # Create Spark DataFrame from Kafka message
        df = spark.createDataFrame([data])

        # Write to Delta Lake (Append mode)
        df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
        print(f"Data saved to Delta Lake at {time.strftime('%Y-%m-%d %H:%M:%S')}")

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'kafka_to_delta_dag',
    default_args=default_args,
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
)

consume_task = PythonOperator(
    task_id='consume_and_save',
    python_callable=consume_and_save_to_delta,
    dag=dag
)
