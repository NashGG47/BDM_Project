#convert data into influx format-- THIS IS TEST CODE
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

# Setup (replace with your actual values)
token = "your-token"
org = "your-org"
bucket = "your-bucket"
url = "http://localhost:8086"

client = InfluxDBClient(url=url, token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Your JSON data
data = {
    "device": "sensor01",
    "temperature": 22.5,
    "humidity": 60,
    "status": "ok",
    "timestamp": "2025-05-12T15:30:00Z"
}

# Create a Point object
point = (
    Point("environment")                     # measurement name
    .tag("device", data["device"])           # tag (indexed)
    .field("temperature", data["temperature"])
    .field("humidity", data["humidity"])
    .field("status", data["status"])         # string field
    .time(data["timestamp"])                 # ISO8601 timestamp
)

# Write to InfluxDB
write_api.write(bucket=bucket, org=org, record=point)
client.close()
