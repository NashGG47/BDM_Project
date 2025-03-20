from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import requests
import json
from datetime import datetime
from PIL import Image
from io import BytesIO

# Inicializar Spark con Delta Lake
builder = SparkSession.builder.appName("GoogleMapsStaticIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Clave de API de Google Maps (Reemplázala con tu propia clave)
GOOGLE_MAPS_API_KEY = "TU_API_KEY_AQUI"

# Definir rutas de almacenamiento
BASE_DIR = "data_ingestion/cold_paths/"
IMAGES_DIR = "storage/delta/raw/images/google_maps_static/"
METADATA_FILE = "storage/delta/raw/metadata/google_maps_static_metadata.json"
DELTA_TABLE_PATH = "storage/delta/raw/google_maps_static/"

# Lista de ubicaciones a capturar (latitud, longitud y zoom)
LOCATIONS = [
    {"name": "barcelona_center", "lat": 41.3874, "lng": 2.1686, "zoom": 14},
    {"name": "sagrada_familia", "lat": 41.4036, "lng": 2.1744, "zoom": 16},
    {"name": "barceloneta", "lat": 41.3786, "lng": 2.1925, "zoom": 15}
]

# Crear directorios si no existen
os.makedirs(IMAGES_DIR, exist_ok=True)
os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)

# Función para descargar imágenes
def download_google_maps_image(location):
    """ Descarga una imagen estática de Google Maps para una ubicación específica """
    url = f"https://maps.googleapis.com/maps/api/staticmap?center={location['lat']},{location['lng']}&zoom={location['zoom']}&size=640x640&maptype=satellite&key={GOOGLE_MAPS_API_KEY}"
    
    response = requests.get(url)
    
    if response.status_code == 200:
        img = Image.open(BytesIO(response.content))
        file_name = f"{location['name']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png"
        file_path = os.path.join(IMAGES_DIR, file_name)
        img.save(file_path, "PNG")
        
        return {
            "location": location["name"],
            "latitude": location["lat"],
            "longitude": location["lng"],
            "zoom": location["zoom"],
            "image_path": file_path,
            "image_width": img.width,
            "image_height": img.height,
            "image_size_kb": os.path.getsize(file_path) / 1024,  # Tamaño en KB
            "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
    else:
        print(f"Error al descargar la imagen de {location['name']}: {response.status_code}")
        return None

# Descargar imágenes y recolectar metadatos
metadata_list = []
for location in LOCATIONS:
    metadata = download_google_maps_image(location)
    if metadata:
        metadata_list.append(metadata)

# Guardar metadatos en JSON
if metadata_list:
    with open(METADATA_FILE, "w") as f:
        json.dump(metadata_list, f, indent=4)
    print(f"Metadatos guardados en {METADATA_FILE}")

# Insertar metadatos en Delta Lake
if metadata_list:
    df = spark.createDataFrame(metadata_list)
    df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
    print(f"Imágenes de Google Maps Static almacenadas en Delta Lake en {DELTA_TABLE_PATH}")

print("Proceso de ingesta de Google Maps Static completado.")
