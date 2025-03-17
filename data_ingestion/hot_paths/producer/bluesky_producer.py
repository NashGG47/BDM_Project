import os
import logging
import time
import json
import requests
from atproto import Client
from dotenv import load_dotenv
from kafka import KafkaProducer
from requests.exceptions import RequestException

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Obtener credenciales desde el archivo .env
BLUESKY_USERNAME = os.getenv("ATP_EMAIL")
BLUESKY_PASSWORD = os.getenv("ATP_PASSWORD")

# Cuentas de BlueSky a monitorear
BLUESKY_ACCOUNTS = ["catalannews.com", "elpais.com"]

# Configuración de Kafka
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'bluesky_data'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Carpeta donde se guardarán imágenes/videos
# SAVE_PATH = "landing_zone/raw/bluesky_images"

# Get the absolute path of the project root directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

# Define the landing zone directory at the project root level
SAVE_PATH = os.path.join(BASE_DIR, "landing_zone", "blue_sky_images")

os.makedirs(SAVE_PATH, exist_ok=True)

def login_to_client():
    """Inicia sesión en BlueSky utilizando credenciales del archivo .env."""
    try:
        if not BLUESKY_USERNAME or not BLUESKY_PASSWORD:
            raise ValueError("Faltan credenciales de BlueSky en el archivo .env.")
        
        client = Client()
        profile = client.login(BLUESKY_USERNAME, BLUESKY_PASSWORD)
        logger.info("✅ Sesión iniciada correctamente en BlueSky.")
        return client
    except Exception as e:
        logger.error("❌ Error al iniciar sesión en BlueSky: %s", e)
        raise

def download_media(media_url, filename):
    """Descarga imágenes o videos de un post de BlueSky y los guarda en SAVE_PATH."""
    try:
        response = requests.get(media_url, stream=True)
        if response.status_code == 200:
            file_path = os.path.join(SAVE_PATH, filename)
            with open(file_path, "wb") as file:
                for chunk in response.iter_content(1024):
                    file.write(chunk)
            logger.info(f"✅ Archivo guardado: {file_path}")
            return file_path  # Retorna la ruta local del archivo descargado
        else:
            logger.warning(f"❌ No se pudo descargar {media_url}. Código: {response.status_code}")
    except Exception as e:
        logger.error(f"⚠️ Error al descargar {media_url}: {e}")
    return None

def fetch_posts_with_media(client, user_handle, max_retries=3, delay=5):
    """Obtiene posts recientes de una cuenta de BlueSky y extrae imágenes/videos."""
    for attempt in range(max_retries):
        try:
            profile_feed = client.get_author_feed(actor=user_handle, limit=10)
            if not profile_feed.feed:
                logger.warning(f"⚠️ La cuenta {user_handle} no tiene publicaciones o no existe.")
                return None
            return profile_feed.feed
        except RequestException as e:
            logger.warning(f"⚠️ Intento {attempt + 1}/{max_retries} fallido al obtener posts de {user_handle}: {e}")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"❌ Error al obtener posts de {user_handle}: {e}")
            return None
    return None

def process_posts(client, user_handle):
    """Procesa publicaciones, descarga imágenes/videos y envía metadatos a Kafka."""
    posts = fetch_posts_with_media(client, user_handle)
    if not posts:
        logger.warning(f"⚠️ No se pudieron obtener posts de {user_handle}.")
        return

    for post in posts:
        post_data = post.post.record
        text_content = post_data.text if hasattr(post_data, "text") else ""
        media_files = []

        # Extraer y descargar imágenes/videos
        if hasattr(post_data, "embed") and hasattr(post_data.embed, "images"):
            for idx, media in enumerate(post_data.embed.images):
                media_url = media.fullsize
                if media_url:
                    filename = f"{user_handle}_{idx}.jpg"  # Cambia extensión si es un video
                    file_path = download_media(media_url, filename)
                    if file_path:
                        media_files.append(file_path)

        # Enviar metadatos a Kafka
        kafka_message = {
            "user": user_handle,
            "text": text_content,
            "media_files": media_files
        }
        producer.send(KAFKA_TOPIC, value=kafka_message)
        logger.info(f"📩 Mensaje enviado a Kafka: {kafka_message}")

def main():
    """Función principal: inicia sesión, obtiene posts, descarga medios y envía datos a Kafka."""
    try:
        client = login_to_client()
        for user in BLUESKY_ACCOUNTS:
            process_posts(client, user)
        
        # Cerrar conexión de Kafka
        producer.flush()
        producer.close()
        logger.info("✅ Proceso completado.")
    except Exception as e:
        logger.error("❌ Error en la ejecución del script: %s", e)

if __name__ == "__main__":
    main()
