from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from processing_co2_semistructured import process_emissions_data
import pandas as pd

def bucket_exists(client, bucket_name):
    buckets_api = client.buckets_api()
    buckets = buckets_api.find_buckets().buckets
    return any(bucket.name == bucket_name for bucket in buckets)

def create_bucket_if_not_exists(client, bucket_name, org):
    if not bucket_exists(client, bucket_name):
        print(f"Bucket '{bucket_name}' not found. Creating it...")
        buckets_api = client.buckets_api()
        buckets_api.create_bucket(bucket_name=bucket_name, org=org)
        print(f"Bucket '{bucket_name}' created successfully.")


def store_co2_dataframe_in_influxdb(df, url, token, org, bucket):
    client = InfluxDBClient(url=url, token=token, org=org)

    # Create bucket if not exists
    buckets_api = client.buckets_api()
    if not any(b.name == bucket for b in buckets_api.find_buckets().buckets):
        print(f"Bucket '{bucket}' not found. Creating it...")
        buckets_api.create_bucket(bucket_name=bucket, org=org)
        print(f"Bucket '{bucket}' created successfully.")

    df['data'] = pd.to_datetime(df['data'])

    write_api = client.write_api(write_options=SYNCHRONOUS)

    for _, row in df.iterrows():
        point = (
            Point("emissions")
            .tag("nom_estacio", row["nom_estacio"])
            .tag("municipi", row["municipi"])
            .tag("contaminant", row["contaminant"])
            .tag("area_urbana", row["area_urbana"])
            .field("average_contaminant", float(row["average_contaminant"]))
            .field("latitud", float(row["latitud"]))
            .field("longitud", float(row["longitud"]))
            .time(row["data"], WritePrecision.NS)
        )
        write_api.write(bucket=bucket, org=org, record=point)

    client.close()
    print(f"Wrote {len(df)} records to InfluxDB bucket '{bucket}'.")

def main():
    grouped_spark_df = process_emissions_data()

    if grouped_spark_df is None:
        print("No data processed, exiting.")
        return

    # Convert Spark DataFrame to Pandas
    pandas_df = grouped_spark_df.toPandas()

    store_co2_dataframe_in_influxdb(
        df=pandas_df,
        url="http://localhost:8086",
        token="token1",
        org="upa",
        bucket="air_quality"
    )

if __name__ == "__main__":
    main()