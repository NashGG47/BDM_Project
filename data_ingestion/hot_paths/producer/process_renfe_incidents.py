import json
import requests
import time
import schedule
from kafka import KafkaProducer
from datetime import datetime


KAFKA_BROKER = "localhost:9092" 
TOPIC = "renfe_incidents"
API_URL = "https://www.renfe.com/content/renfe/es/es/grupo-renfe/comunicacion/renfe-al-dia/avisos/jcr:content/root/responsivegrid/rfincidentreports_co.noticeresults.json"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_send():
    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()

        if not data:
            print(f"[{datetime.now()}] No new data found.")
            return
        
        for record in data:
            record["timestamp"] = datetime.now().isoformat()
            producer.send(TOPIC, record)
        
        print(f"[{datetime.now()}] Sent {len(data)} incidents to Kafka.")

    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")

schedule.every(1).hours.do(fetch_and_send)
#fetch_and_send()

while True:
    schedule.run_pending()
    time.sleep(60)
