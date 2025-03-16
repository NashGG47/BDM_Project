from kafka import KafkaConsumer
import json
import time
from pyspark.sql import SparkSession

# Function to set up Spark session for Delta Lake
def get_spark_session():
    spark = SparkSession.builder \
        .appName("DeltaLakeWriter") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    return spark

# Kafka Consumer setup
consumer = KafkaConsumer(
    'emissions_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

spark = get_spark_session()
delta_path = "storage/delta/raw/emissions_data"  # Adjust path if necessary

print(f"Consuming data from Kafka and writing to Delta Lake at path: {delta_path}")

for message in consumer:
    data = [message.value]  # Wrap message value in a list for DataFrame creation
    df = spark.createDataFrame(data)
    
    # Write data to Delta Lake (append mode)
    df.write.format("delta").mode("append").save(delta_path)
    
    print(f"Data received at {time.strftime('%Y-%m-%d %H:%M:%S')}: {message.value}")
    print(f"Data written to Delta Lake at {delta_path}\n")
