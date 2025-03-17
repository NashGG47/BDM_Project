
#This script uses the SerpAPI to fetch Google Maps images and send them to Kafka.

import os
import requests
from kafka import KafkaProducer
import json
from serpapi import GoogleSearch
from dotenv import load_dotenv

# Load environment variables from .env
load_dotenv()

# Get API key from environment variable
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# Kafka setup
KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'google_maps_images'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Get the absolute path of the project root directory
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))

# Define the landing zone directory at the project root level
LANDING_ZONE_RAW = os.path.join(BASE_DIR, "landing_zone", "google_maps_images")

os.makedirs(LANDING_ZONE_RAW, exist_ok=True)

def fetch_google_maps_images(query):
    """Fetch Google Maps images using SerpAPI."""
    params = {
        "q": query,
        "tbm": "isch",  # Image search
        "api_key": SERPAPI_KEY
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    return results.get("images_results", [])

def download_image(image_url, save_path):
    """Download and save an image from a URL."""
    try:
        response = requests.get(image_url)
        if response.status_code == 200:
            with open(save_path, 'wb') as file:
                file.write(response.content)
            print(f"Image saved: {save_path}")
        else:
            print(f"Failed to download image: {image_url}")
    except Exception as e:
        print(f"Error downloading image: {e}")

def send_to_kafka():
    """Fetch Google Maps images and send data to Kafka."""
    query = "Barcelona traffic"
    images = fetch_google_maps_images(query)

    for idx, image in enumerate(images):
        image_url = image["thumbnail"]
        image_filename = f"google_maps_image_{idx}.jpg"
        image_path = os.path.join(LANDING_ZONE_RAW, image_filename)

        # Download the image
        download_image(image_url, image_path)

        # Send image metadata to Kafka
        image_data = {
            "image_url": image_url,
            "image_path": image_path,
            "query": query
        }
        producer.send(KAFKA_TOPIC, value=image_data)
    
    producer.flush()  # Ensures all messages are sent before closing
    producer.close()  # Closes the producer safely

    print("Google Maps images sent to Kafka!")

if __name__ == "__main__":
    send_to_kafka()