import os
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
from trusted_zone.processing_transit_semistructured import process_gencat_incidents


def store_incidents_in_influxdb(processed_data, url, token, org, bucket):

    client = InfluxDBClient(url=url, token=token, org=org)
    write_api = client.write_api(write_options=SYNCHRONOUS)

    for record in processed_data:
        try:
            p = Point(record["measurement"]) \
                .tag("incident_id", record["tags"]["incident_id"]) \
                .tag("road", record["tags"]["road"]) \
                .tag("cause", record["tags"]["cause"]) \
                .tag("direction", record["tags"]["direction"]) \
                .field("pk_start", float(record["fields"]["pk_start"])) \
                .field("pk_end", float(record["fields"]["pk_end"])) \
                .field("lat", float(record["fields"]["lat"])) \
                .field("lon", float(record["fields"]["lon"])) \
                .field("description", record["fields"]["description"]) \
                .time(datetime.strptime(record["fields"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ"), WritePrecision.NS)

            write_api.write(bucket=bucket, org=org, record=p)

        except Exception as e:
            print(f"Failed to write record: {record}")
            print(f"Error: {e}")

    print("Data written to InfluxDB successfully!")
    client.close()

processed_data = process_gencat_incidents()

if processed_data:
    store_incidents_in_influxdb(
        processed_data,
        url="http://localhost:8086",
        token="token1", 
        org="upa",     
        bucket="gencat_incidents"
    )
else:
    print("No incident data to write.")
