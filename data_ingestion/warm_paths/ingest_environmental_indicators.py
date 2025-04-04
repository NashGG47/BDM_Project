import os
import json
import time
import requests
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("EnvironmentalIndicatorsIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

BASE_DIR = "data_ingestion/warm_paths/"
DELTA_TABLE_PATH = "storage/delta/raw/environmental_indicators/"
METADATA_FILE = "storage/delta/raw/metadata/environmental_indicators.json"

DATASET_NAME = "environmental_indicators"
DATASET_INFO = {
    "url": "https://data.renfe.com/dataset/81f21f5f-b093-413d-92a8-e9f5e670cd6b/resource/5d987311-277c-454f-bfbc-f81a32326973/download/principales-indicadores-ambientales.xls",
    "format": "xls"
}

def download_and_save():
    file_format = DATASET_INFO["format"]
    url = DATASET_INFO["url"]
    year_month = datetime.now().strftime('%Y%m')  
    local_path = os.path.join(BASE_DIR, DATASET_NAME, year_month, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading {DATASET_NAME} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Saved: {local_path}")
    return local_path

# Function to process XLS files for Environmental Indicators
def process_xls(file_path):
    df = pd.read_excel(file_path)
    column_mapping = {
        "Año": "year",
        "GWh traccion electrica": "gwh_traccion_electrica",
        "Millones de litros de diesel consumidos": "millones_litros_diesel_consumidos",
        "GWh L diesel": "gwh_l_diesel",
        "GWh total": "gwh_total",
        "Intensidad Energetica (Wh UT)": "intensidad_energetica_wh_ut",
        "Intensidad de Carbono (g CO2 UT)": "intensidad_carbono_g_co2_ut",
        "Gastos e inversiones ambientales (miles de euros)": "gastos_inversiones_ambientales_euros",
        "Consumo de agua (m3)": "consumo_agua_m3",
        "Generacion de residuos peligrosos (toneladas)": "generacion_residuos_peligrosos_toneladas",
        "Porcentaje Trafico Viajeros con trenes baja emision acustica": "porcentaje_trafico_viajeros_trenes_baja_emision_acustica",
        "Porcentaje Trafico Mercancias con trenes baja emision acustica": "porcentaje_trafico_mercancias_trenes_baja_emision_acustica"
    }
    df.columns = df.columns.str.strip()
    df.rename(columns=column_mapping, inplace=True)
    df.columns = df.columns.str.strip().str.replace(" ", "_").str.replace(r"[;{}()\n\t=]", "", regex=True)
    return df

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
        "processing_duration_seconds": processing_duration
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
    df = process_xls(raw_file_path)
    delta_path, row_count = store_in_delta(df)
    processing_duration = time.time() - start_time
    log_metadata(raw_file_path, DATASET_INFO["url"], file_size, delta_path, row_count, processing_duration)
    print("Environmental Indicators Ingestion Complete!")
