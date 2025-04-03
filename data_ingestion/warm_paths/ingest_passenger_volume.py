import os
import json
import time
import requests
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("PassengerVolumeIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

BASE_DIR = "data_ingestion/warm_paths/"
DELTA_TABLE_PATH = "storage/delta/raw/passenger_volume/"
METADATA_FILE = "storage/delta/raw/metadata/passenger_volume.json"

DATASET_NAME = "passenger_volume"
DATASET_INFO = {
    "url": "https://data.renfe.com/dataset/9190f983-e138-42da-901a-b37205562fe4/resource/1417396e-4d6a-466a-a987-03d07aa92bed/download/barcelona_viajeros_por_franja_csv.csv",
    "format": "csv"
}

def download_and_save():
    file_format = DATASET_INFO["format"]
    url = DATASET_INFO["url"]
    year_month = datetime.now().strftime('%Y%m')  # e.g. "202504"
    local_path = os.path.join(BASE_DIR, DATASET_NAME, year_month, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading {DATASET_NAME} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Saved: {local_path}")
    return local_path

# Function to process CSV files for Passenger Volume
def process_csv(file_path):
    try:
        df = pd.read_csv(file_path)
        df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[;{}()\n\t=]", "", regex=True)
        return df
    except Exception as e:
        print(f"Error processing CSV {file_path}: {e}")
        return pd.DataFrame()  # Return empty DataFrame on failure

def store_in_delta(df):
    if df.empty:
        print(f"Skipping {DATASET_NAME}: No data to store.")
        return None, 0
    print(f"Storing {DATASET_NAME} in Delta Lake...")
    current_ym = datetime.now().strftime('%Y%m')    # e.g. "202504"
    current_ymd = datetime.now().strftime('%Y%m%d')  # e.g. "20250403"
    final_delta_path = os.path.join(DELTA_TABLE_PATH, current_ym, current_ymd)
    os.makedirs(final_delta_path, exist_ok=True)
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").save(final_delta_path)
    print(f"Delta Table Updated: {final_delta_path}")
    row_count = len(df)
    return final_delta_path, row_count

def log_metadata(raw_file_path, source_url, file_size, delta_path, row_count, processing_duration):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)
    log_entry = {
        "dataset": DATASET_NAME,
        "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        "raw_file_path": raw_file_path,
        "source_url": source_url,
        "file_size_bytes": file_size,
        "delta_path": delta_path,
        "row_count": row_count,
        "processing_duration_seconds": processing_duration,
        "columns": None  # Optionally, you could record the column names here
    }
    if os.path.exists(METADATA_FILE):
        with open(METADATA_FILE, "r") as f:
            logs = json.load(f)
    else:
        logs = []
    logs.append(log_entry)
    with open(METADATA_FILE, "w") as f:
        json.dump(logs, f, indent=4)
    print(f"Metadata logged for {DATASET_NAME}")

if __name__ == "__main__":
    start_time = time.time()
    raw_file_path = download_and_save()
    file_size = os.path.getsize(raw_file_path)
    df = process_csv(raw_file_path)
    delta_path, row_count = store_in_delta(df)
    processing_duration = time.time() - start_time
    log_metadata(raw_file_path, DATASET_INFO["url"], file_size, delta_path, row_count, processing_duration)
    print("Passenger Volume Ingestion Complete!")
