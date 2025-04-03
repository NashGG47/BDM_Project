import requests
import json
from kafka import KafkaProducer
import schedule
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'emissions_data'
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_and_send():
    url = "https://analisi.transparenciacatalunya.cat/resource/tasf-thgu.json"
    
    try:
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            producer.send(TOPIC, value=data)
            producer.flush()  # Ensure all messages are sent
            print(f"Data sent to Kafka topic '{TOPIC}' at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        else:
            print(f"Failed to retrieve data: {response.status_code}")
    
    except Exception as e:
        print(f"Error occurred: {e}")

schedule.every(10).seconds.do(fetch_and_send)

print("Kafka producer started, fetching data every hour...")
while True:
    schedule.run_pending()
    time.sleep(1)
