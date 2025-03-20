from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from kafka import KafkaConsumer
import os
import json
import requests
from datetime import datetime

# Inicializar Spark con Delta Lake
builder = SparkSession.builder.appName("BlueskyConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'bluesky_data'

# Definir rutas de almacenamiento
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
IMAGES_DIR = os.path.join(BASE_DIR, "storage", "delta", "raw", "images", "bluesky")
DELTA_TABLE_PATH = os.path.join(BASE_DIR, "storage", "delta", "raw", "bluesky")
METADATA_FILE = os.path.join(BASE_DIR, "storage", "delta", "raw", "metadata", "bluesky_metadata.json")

# Crear directorios si no existen
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)

# Inicializar Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    value_deserializer=lambda v: json.loads(v.decode('utf-8'))
)

def download_image(image_url, user_handle, index):
    """Descarga imágenes de los posts de BlueSky y las guarda en la carpeta de imágenes."""
    try:
        response = requests.get(image_url, stream=True)
        if response.status_code == 200:
            file_name = f"{user_handle}_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{index}.jpg"
            file_path = os.path.join(IMAGES_DIR, file_name)

            with open(file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            
            print(f"✅ Imagen guardada: {file_path}")
            return file_path
        else:
            print(f"❌ No se pudo descargar {image_url}. Código: {response.status_code}")
    except Exception as e:
        print(f"⚠️ Error al descargar {image_url}: {e}")
    return None

def process_message(message):
    """Procesa cada mensaje de Kafka, descarga imágenes y almacena metadatos en Delta Lake."""
    user_handle = message.get("user")
    text_content = message.get("text", "")
    media_urls = message.get("media_files", [])

    media_files = []
    for idx, url in enumerate(media_urls):
        local_path = download_image(url, user_handle, idx)
        if local_path:
            media_files.append(local_path)

    metadata = {
        "user": user_handle,
        "text": text_content,
        "media_files": media_files,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }

    # Guardar metadatos en JSON
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(metadata)

    with open(METADATA_FILE, "w") as f:
        json.dump(logs, f, indent=4)
    
    print(f"📜 Metadata guardada en {METADATA_FILE}")

    # Guardar en Delta Lake
    df = spark.createDataFrame([metadata])
    df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
    print(f"✅ Datos almacenados en Delta Lake: {DELTA_TABLE_PATH}")

def main():
    """Lee mensajes desde Kafka y procesa cada post."""
    print("📥 Iniciando Consumer de BlueSky...")
    for message in consumer:
        process_message(message.value)

if __name__ == "__main__":
    main()
