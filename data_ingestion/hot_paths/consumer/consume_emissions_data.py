from kafka import KafkaConsumer
import json
import time

consumer = KafkaConsumer(
    'emissions_data',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

for message in consumer:
    print(f"Received at {time.strftime('%Y-%m-%d %H:%M:%S')}: {message.value}")
