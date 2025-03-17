from kafka import KafkaProducer
import json
import requests
import xmltodict

KAFKA_BROKER = 'localhost:9092'  # Kafka broker address
KAFKA_TOPIC = 'transit_incidents'  # Kafka topic to send data to
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
def fetch_gml_data():
    url = "https://www.gencat.cat/transit/opendata/incidenciesGML.xml"
    response = requests.get(url)
    gml_data = xmltodict.parse(response.content)

    incidents = []
    for feature in gml_data["wfs:FeatureCollection"]["gml:featureMember"]:
        data = feature["cite:mct2_v_afectacions_data"]
        incidents.append({
            "id": int(data["cite:identificador"]),
            "type": int(data["cite:tipus"]),
            "road": data["cite:carretera"],
            "pk_start": int(data["cite:pk_inici"]),
            "pk_end": int(data["cite:pk_fi"]),
            "cause": data["cite:causa"],
            "level": int(data["cite:nivell"]),
            "destination": data["cite:cap_a"],
            "direction": data["cite:sentit"],
            "description": data["cite:descripcio"],
            "latitude": float(data["cite:geom"]["gml:Point"]["gml:coordinates"].split(",")[1]),
            "longitude": float(data["cite:geom"]["gml:Point"]["gml:coordinates"].split(",")[0]),
            "timestamp": data["cite:data"],
            "source": data["cite:font"]
        })

    return incidents
def send_to_kafka():
    incidents = fetch_gml_data()
    for incident in incidents:
        producer.send(KAFKA_TOPIC, value=incident)
    print("Data sent to Kafka!")

send_to_kafka()
