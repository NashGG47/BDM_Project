from kafka import KafkaProducer
import json
import requests
import xmltodict
import schedule
from datetime import datetime
import time
import logging

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'gencat_incidents'

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    handlers=[logging.StreamHandler()]
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_gml_data():
    url = "https://www.gencat.cat/transit/opendata/incidenciesGML.xml"
    
    try:
        logging.info("Requesting data from API...")
        response = requests.get(url, timeout=(5, 10)) 
        response.raise_for_status()
        logging.info(f"API Response Code: {response.status_code}")
        logging.debug(f"Response content: {response.content[:200]}")  

    except requests.RequestException as e:
        logging.error(f"API Error: {e}")
        return []

    try:
        gml_data = xmltodict.parse(response.content)
        features = gml_data["wfs:FeatureCollection"].get("gml:featureMember", [])

        if not isinstance(features, list):
            features = [features]

        incidents = []
        for feature in features:
            data = feature.get("cite:mct2_v_afectacions_data", {})

            coordinates = data.get("cite:geom", {}).get("gml:Point", {}).get("gml:coordinates")
            if coordinates and isinstance(coordinates, dict):
                coord_text = coordinates.get("#text")
                if coord_text:
                    try:
                        lon_str, lat_str = coord_text.split(",")
                        latitude = float(lat_str)
                        longitude = float(lon_str)
                    except ValueError as e:
                        latitude = longitude = None
                        logging.error(f"Error parsing coordinates '{coord_text}': {e}")
                else:
                    latitude = longitude = None
                    logging.warning(f"Missing coordinate text: {coordinates}")
            else:
                latitude = longitude = None
                logging.warning(f"Missing or invalid coordinates: {coordinates}")
            incident = {
                "id": int(data.get("cite:identificador", 0)),
                "type": int(data.get("cite:tipus", 0)),
                "road": data.get("cite:carretera", ""),
                "pk_start": float(data.get("cite:pk_inici", 0.0)),
                "pk_end": float(data.get("cite:pk_fi", 0.0)),
                "cause": data.get("cite:causa", ""),
                "level": int(data.get("cite:nivell", 0)),
                "destination": data.get("cite:cap_a", "Unknown"),
                "direction": data.get("cite:sentit", ""),
                "description": data.get("cite:descripcio", ""),
                "latitude": latitude,
                "longitude": longitude,
                "timestamp": data.get("cite:data", ""),
                "source": data.get("cite:font", "")
            }
            
            incidents.append(incident)

        return incidents

    except Exception as e:
        logging.error(f"Error parsing API response: {e}")
        return []


def send_to_kafka():
    logging.info("Attempting to send data to Kafka...")

    incidents = fetch_gml_data()
    
    if not incidents:
        logging.info("No incidents to send.")
        return

    for incident in incidents:
        try:
            producer.send(KAFKA_TOPIC, value=incident).get(timeout=10)
        except Exception as e:
            logging.error(f"Kafka Error: {e}")

    producer.flush()
    logging.info(f"Sent {len(incidents)} incident(s) to Kafka.")
schedule.every(1).minutes.do(send_to_kafka)

logging.info("Scheduler started. Fetching data every 1 minute.")
while True:
    schedule.run_pending()
    time.sleep(1)
