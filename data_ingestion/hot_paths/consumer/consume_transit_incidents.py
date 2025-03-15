from kafka import KafkaConsumer
import json
from delta import DeltaTable
import os
from pyspark.sql import SparkSession
import pandas as pd
import time

# Kafka Consumer setup
KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
KAFKA_TOPIC = 'transit_incidents'  # Kafka topic to consume from
DELTA_PATH = "/storage/delta/transit"  # Path to store the Delta table

# Initialize Spark session
spark = SparkSession.builder \
    .appName("TransitIncidentsConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

def consume_kafka_messages():
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',  # Adjust this based on your needs
        enable_auto_commit=True,
        group_id='transit-consumer-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    batch = []
    batch_start_time = time.time()

    # Run indefinitely, consume in batches every hour
    while True:
        for message in consumer:
            # Collect messages in a batch
            batch.append(message.value)

            # If one hour has passed, process the batch and clear it
            if time.time() - batch_start_time >= 3600:  # 3600 seconds = 1 hour
                process_batch(batch)
                batch = []  # Reset the batch
                batch_start_time = time.time()  # Reset the batch start time

def process_batch(batch):
    """Process the batch of messages."""
    try:
        # Convert the batch to a DataFrame
        df = pd.DataFrame(batch)  # Convert the batch (list of dicts) to a DataFrame
        
        # Convert the pandas DataFrame to a Spark DataFrame
        spark_df = spark.createDataFrame(df)
        
        # Write data to Delta Lake
        write_to_delta(spark_df)

    except Exception as e:
        print(f"Error processing batch: {e}")

def write_to_delta(df):
    """Write processed data to Delta Lake."""
    try:
        if os.path.exists(DELTA_PATH):
            delta_table = DeltaTable.forPath(spark, DELTA_PATH)  # Using the DeltaTable API to write
            delta_table.alias("old").merge(
                df.alias("new"), "old.id = new.id"
            ).whenMatchedUpdate(set={
                "id": "new.id", 
                "type": "new.type", 
                "road": "new.road", 
                "pk_start": "new.pk_start", 
                "pk_end": "new.pk_end", 
                "cause": "new.cause", 
                "level": "new.level", 
                "destination": "new.destination", 
                "description": "new.description",
                "direction": "new.direction",
                "latitude": "new.latitude", 
                "longitude": "new.longitude", 
                "timestamp": "new.timestamp"
            }).whenNotMatchedInsertAll().execute()
        else:
            # If Delta table doesn't exist, create a new one
            df.write.format("delta").mode("append").save(DELTA_PATH)
    except Exception as e:
        print(f"Error saving data to Delta: {e}")

# Start consuming messages from Kafka in batches every hour
consume_kafka_messages()



