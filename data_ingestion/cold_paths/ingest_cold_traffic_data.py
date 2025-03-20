from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import pandas as pd
import json
import requests
from datetime import datetime

# Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("ColdPathIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define Storage Paths
BASE_DIR = "storage/delta/raw/traffic_data/2025/"
DELTA_TABLE_PATH = "storage/delta/raw/traffic_data/2024/delta_table/"
METADATA_FILE = "storage/delta/raw/metadata/ingestion_logs.json"

# Define Cold Path Data Source
data_source = {
    "traffic_data": {
        "url": "https://opendata-ajuntament.barcelona.cat/data/dataset/1dffc2aa-882e-4765-bb98-9f77e1b21d4a/resource/57968ea7-f7da-43ce-ae8b-54fd8dd30b5f/download",
        "format": "csv"
    }
}

# Function to Download and Save Data
def download_and_save(dataset_name, dataset_info):
    file_format = dataset_info["format"]
    url = dataset_info["url"]
    local_path = os.path.join(BASE_DIR, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading {dataset_name} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Saved: {local_path}")
    return local_path

# Function to Process CSV
def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[;{}()\n\t=]", "", regex=True)
        return df
    except Exception as e:
        print(f"Error processing CSV {file_path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

# Function to Store Data in Delta Lake
def store_in_delta(dataset_name, df):
    if df.empty:
        print(f"Skipping {dataset_name}: No data to store.")
        return

    print(f"Storing {dataset_name} in Delta Lake...")
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").save(DELTA_TABLE_PATH)
    print(f"Delta Table Updated: {DELTA_TABLE_PATH}")

# Function to Log Metadata
def log_metadata(dataset_name, file_path):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    
    log_entry = {
        "dataset": dataset_name,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "file_path": file_path
    }

    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            logs = json.load(f)
    else:
        logs = []

    logs.append(log_entry)

    with open(METADATA_FILE, "w") as f:
        json.dump(logs, f, indent=4)

    print(f"Metadata logged for {dataset_name}")

# Execute Ingestion for Cold Path Dataset
for dataset_name, dataset_info in data_source.items():
    local_file = download_and_save(dataset_name, dataset_info)
    df = process_csv(local_file)
    store_in_delta(dataset_name, df)
    log_metadata(dataset_name, local_file)

print("Cold Path Data Ingestion Complete!")
