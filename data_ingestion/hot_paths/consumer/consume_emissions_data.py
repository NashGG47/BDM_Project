from kafka import KafkaConsumer
import json
import time
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd
from datetime import datetime

KAFKA_TOPIC = "emissions_data"
KAFKA_BROKER = "localhost:9092"

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

builder = SparkSession.builder.appName("WarmPathIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

DELTA_TABLE_PATHS = {
    "emissions_data": "storage/delta/raw/emissions_data",
}
METADATA_FILE = "storage/delta/raw/metadata/emissions_data.json"

def store_in_delta(dataset_name, df):
    """
    Store DataFrame in Delta Lake
    """
    if df.empty:
        print(f"No data to store for {dataset_name}.")
        return
    
    print(f"Storing {dataset_name} in Delta Lake...")
    delta_path = DELTA_TABLE_PATHS.get(dataset_name, "storage/delta/raw/misc/")
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").save(delta_path)
    print(f"Delta Table Updated: {delta_path}")

def log_metadata(dataset_name, count):
    """
    Log metadata about the data ingestion process
    """
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)

    log_entry = {
        "dataset": dataset_name,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "records_ingested": count
    }
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(log_entry)
    with open(METADATA_FILE, "w") as f:
        json.dump(logs, f, indent=4)

    print(f"Metadata logged for {dataset_name}")

def process_messages():
    """
    Process Kafka messages and save to Delta Lake
    """
    batch_data = []
    for message in consumer:
        incident = message.value
        print(f"Received at {time.strftime('%Y-%m-%d %H:%M:%S')}: {incident}")
        batch_data.append(incident)
        if len(batch_data) >= 10:
            df = pd.DataFrame(batch_data)
            store_in_delta("emissions_data", df)
            log_metadata("emissions_data", len(batch_data))
            batch_data = []
if __name__ == "__main__":
    print("Starting Kafka Consumer...")
    process_messages()