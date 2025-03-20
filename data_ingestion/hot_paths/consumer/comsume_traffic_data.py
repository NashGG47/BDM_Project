from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from kafka import KafkaConsumer
import os
import json
from datetime import datetime

# Inicializar Spark con Delta Lake
builder = SparkSession.builder.appName("TrafficConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configuración de Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "traffic_data"

# Definir rutas de almacenamiento
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
DELTA_TABLE_PATH = os.path.join(BASE_DIR, "storage", "delta", "raw", "traffic_data")
METADATA_FILE = os.path.join(BASE_DIR, "storage", "delta", "raw", "metadata", "traffic_metadata.json")

# Crear directorios si no existen
os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)

# Inicializar Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def store_in_delta(traffic_data):
    """Almacena los datos de tráfico en Delta Lake."""
    df = spark.createDataFrame([traffic_data])
    df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
    print(f"✅ Datos almacenados en Delta Lake: {DELTA_TABLE_PATH}")

def log_metadata(traffic_data):
    """Guarda metadatos del tráfico en JSON."""
    metadata_entry = {
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "data": traffic_data
    }

    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(metadata_entry)

    with open(METADATA_FILE, "w") as f:
        json.dump(logs, f, indent=4)

    print(f"📜 Metadata guardada en {METADATA_FILE}")

def main():
    """Lee mensajes desde Kafka y almacena en Delta Lake."""
    print("📥 Iniciando Consumer de tráfico...")
    for message in consumer:
        traffic_data = message.value
        store_in_delta(traffic_data)
        log_metadata(traffic_data)

if __name__ == "__main__":
    main()
