import os
import json
import requests
import time
import csv
from kafka import KafkaProducer

# Configuración
URL = "https://opendata-ajuntament.barcelona.cat/data/dataset/1dffc2aa-882e-4765-bb98-9f77e1b21d4a/resource/1649682d-3b24-42f2-ba4f-807695dce537/download"
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "traffic_data"

# Inicializar Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def fetch_traffic_data():
    """Descarga los datos de tráfico desde OpenData Barcelona y los transmite a Kafka."""
    try:
        response = requests.get(URL)
        if response.status_code != 200:
            print(f"❌ Error al descargar datos: {response.status_code}")
            return

        # Guardar temporalmente el CSV
        csv_filename = "traffic_data.csv"
        with open(csv_filename, "wb") as file:
            file.write(response.content)
        
        print("✅ Datos de tráfico descargados.")

        # Leer CSV y enviar cada fila a Kafka como JSON
        with open(csv_filename, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                producer.send(KAFKA_TOPIC, value=row)
                print(f"📤 Enviado a Kafka: {row}")
                time.sleep(0.5)  # Simulación de streaming

        print("✅ Streaming de datos finalizado.")
    except Exception as e:
        print(f"❌ Error en el productor: {e}")

if __name__ == "__main__":
    fetch_traffic_data()
