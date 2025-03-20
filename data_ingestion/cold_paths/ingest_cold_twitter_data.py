from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import requests
import json
from datetime import datetime
from PIL import Image
from io import BytesIO

# Inicializar Spark con Delta Lake
builder = SparkSession.builder.appName("TwitterDataIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Definir credenciales de API de Twitter (Reemplázalas con las tuyas)
BEARER_TOKEN = "TU_TWITTER_BEARER_TOKEN"

# Definir rutas de almacenamiento
BASE_DIR = "data_ingestion/cold_paths/"
IMAGES_DIR = "storage/delta/raw/images/twitter/"
METADATA_FILE = "storage/delta/raw/metadata/twitter_metadata.json"
DELTA_TABLE_PATH = "storage/delta/raw/twitter/"

# Crear directorios si no existen
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)

# Definir parámetros de búsqueda (puedes modificar el query)
QUERY = "Barcelona traffic has:images -is:retweet"
MAX_RESULTS = 10  # Número de tweets a recuperar

# Función para obtener tweets con imágenes
def fetch_twitter_data():
    url = "https://api.twitter.com/2/tweets/search/recent"
    headers = {"Authorization": f"Bearer {BEARER_TOKEN}"}
    params = {
        "query": QUERY,
        "max_results": MAX_RESULTS,
        "tweet.fields": "id,text,created_at",
        "expansions": "attachments.media_keys",
        "media.fields": "url,width,height"
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error al obtener tweets: {response.status_code} - {response.text}")
        return None

# Función para descargar y guardar imágenes
def download_twitter_image(image_url, tweet_id):
    response = requests.get(image_url)

    if response.status_code == 200:
        img = Image.open(BytesIO(response.content))
        file_name = f"{tweet_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.jpg"
        file_path = os.path.join(IMAGES_DIR, file_name)
        img.save(file_path, "JPEG")

        return {
            "image_path": file_path,
            "image_width": img.width,
            "image_height": img.height,
            "image_size_kb": os.path.getsize(file_path) / 1024  # Tamaño en KB
        }
    else:
        print(f"Error al descargar imagen: {image_url}")
        return None

# Procesar los datos obtenidos
metadata_list = []
tweets_data = fetch_twitter_data()

if tweets_data and "data" in tweets_data:
    media_dict = {m["media_key"]: m for m in tweets_data.get("includes", {}).get("media", [])}

    for tweet in tweets_data["data"]:
        media_keys = tweet.get("attachments", {}).get("media_keys", [])

        for media_key in media_keys:
            media_info = media
