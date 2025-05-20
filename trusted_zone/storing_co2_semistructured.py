from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from processing_co2_semistructured import process_emissions_data

def bucket_exists(client, bucket_name):
    buckets_api = client.buckets_api()
    buckets = buckets_api.find_buckets().buckets
    return any(bucket.name == bucket_name for bucket in buckets)

def store_co2_in_influxdb(processed_data, url, token, org, bucket):
    client = InfluxDBClient(url=url, token=token, org=org)

    if not bucket_exists(client, bucket):
        print(f"Bucket '{bucket}' does not exist in org '{org}'. Aborting write.")
        client.close()
        return

    write_api = client.write_api(write_options=SYNCHRONOUS)

    for record in processed_data:
        point = Point(record["measurement"]) \
            .tag("station", record["tags"]["station"]) \
            .tag("municipality", record["tags"]["municipality"]) \
            .tag("contaminant", record["tags"]["contaminant"]) \
            .tag("unit", record["tags"]["unit"]) \
            .tag("area", record["tags"]["area"]) \
            .tag("eoi_code", record["tags"]["eoi_code"]) \
            .field("value", record["fields"]["value"]) \
            .field("lat", record["fields"]["lat"]) \
            .field("lon", record["fields"]["lon"]) \
            .time(record["time"], WritePrecision.NS)
        write_api.write(bucket=bucket, org=org, record=point)

    client.close()
    print(f"Wrote {len(processed_data)} records to InfluxDB bucket '{bucket}'.")

# Call processing and storage
processed_data = process_emissions_data()

if processed_data:
    store_co2_in_influxdb(
        processed_data,
        url="http://localhost:8086",
        token="token1", 
        org="upa",     
        bucket="air_quality"
    )
else:
    print("No incident data to write.")
