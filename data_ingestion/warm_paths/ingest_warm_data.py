from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import pandas as pd
import json
import requests
from datetime import datetime

# 🚀 Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("WarmPathIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# 🚀 Define Storage Paths
BASE_DIR = "data_ingestion/warm_paths/"
DELTA_TABLE_PATH = "data_ingestion/delta_storage/warm_paths/"
METADATA_FILE = "data_ingestion/metadata/ingestion_logs.json"

# 🚀 Define Warm Path Data Sources
warm_path_datasets = {
    "environmental_indicators": "https://data.renfe.com/dataset/81f21f5f-b093-413d-92a8-e9f5e670cd6b/resource/5d987311-277c-454f-bfbc-f81a32326973/download/principales-indicadores-ambientales.xls",
    "passenger_volume": "https://data.renfe.com/dataset/9190f983-e138-42da-901a-b37205562fe4/resource/1417396e-4d6a-466a-a987-03d07aa92bed/download/barcelona_viajeros_por_franja_csv.csv"
}

# 🚀 Function to Download and Save Data
def download_and_save(dataset_name, url, file_format):
    local_path = os.path.join(BASE_DIR, dataset_name, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"📥 Downloading {dataset_name} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"✅ Saved: {local_path}")
    return local_path

# 🚀 Function to Process XLS (for Environmental Indicators)
def process_xls(file_path):
    df = pd.read_excel(file_path)

    # Standardize column names for Delta Lake compatibility
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

    # Remove leading/trailing spaces in column names
    df.columns = df.columns.str.strip()

    # Apply column renaming if a mapping exists
    df.rename(columns=column_mapping, inplace=True)

    # Ensure all column names are clean
    df.columns = (
        df.columns.str.strip()  # Remove extra spaces
                 .str.replace(" ", "_")  # Replace spaces with underscores
                 .str.replace(r"[;{}()\n\t=]", "", regex=True)  # Remove special characters
    )
    
    return df

# 🚀 Function to Process CSV (for Passenger Volume)
def process_csv(file_path):
    return pd.read_csv(file_path)

# 🚀 Function to Store Data in Delta Lake
def store_in_delta(dataset_name, df):
    print(f"📤 Storing {dataset_name} in Delta Lake...")
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").save(DELTA_TABLE_PATH + dataset_name)
    print(f"✅ Delta Table Updated: {DELTA_TABLE_PATH + dataset_name}")

# 🚀 Function to Log Metadata
def log_metadata(dataset_name, file_path):
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
    print(f"📜 Metadata logged for {dataset_name}")

# Execute Ingestion for Warm Path Datasets
for dataset, url in warm_path_datasets.items():
    file_format = "xls" if "xls" in url else "csv"
    local_file = download_and_save(dataset, url, file_format)
    df = process_xls(local_file) if file_format == "xls" else process_csv(local_file)
    store_in_delta(dataset, df)
    log_metadata(dataset, local_file)

print("🎯 Warm Path Data Ingestion Complete!")
