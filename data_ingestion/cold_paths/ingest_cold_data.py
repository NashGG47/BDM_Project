from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import os
import geopandas as gpd
import json
import requests
import zipfile
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("ColdPathIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define Storage Paths
BASE_DIR = "data_ingestion/cold_paths/"
DELTA_TABLE_PATH = "storage/delta/raw/"
METADATA_FILE = "storage/delta/raw/metadata/ingestion_logs.json"

# Define Cold Path Data Sources
cold_path_datasets = {
    "administrative_shapefiles": "https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/resource/11851135-6919-4dcb-91ed-821e5e87a428/download"
}

# Function to Download and Save Data
def download_and_save(dataset_name, url, file_format):
    local_path = os.path.join(BASE_DIR, dataset_name, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading {dataset_name} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Saved: {local_path}")
    return local_path

# Function to Extract and Read Shapefiles from ZIP
def extract_shapefile(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    # Find the first .shp file in the extracted directory
    for file in os.listdir(extract_to):
        if file.endswith(".shp"):
            return os.path.join(extract_to, file)
    return None

# Function to Store Data in Delta Lake
def store_in_delta(dataset_name, gdf):
    print(f"Storing {dataset_name} in Delta Lake...")

    # Convert the geometry column to WKT (Well-Known Text)
    gdf["geometry_wkt"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom else None)
    df = gdf.drop(columns=["geometry"])  # Remove the original geometry column

    # Debugging: Print column data types
    print("Column Data Types Before Conversion:")
    print(df.dtypes)

    # Convert all columns to string to avoid type inference issues
    df = df.astype(str)

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(df)

    spark_df.write.format("delta").mode("append").save(DELTA_TABLE_PATH + dataset_name)
    print(f"Delta Table Updated: {DELTA_TABLE_PATH + dataset_name}")

# Function to Log Metadata
def log_metadata(dataset_name, file_path):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True)  # Ensure metadata directory exists

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

for dataset, url in cold_path_datasets.items():
    zip_file = download_and_save(dataset, url, "zip")
    extracted_shp = extract_shapefile(zip_file, os.path.join(BASE_DIR, dataset))
    if extracted_shp:
        store_in_delta(dataset, gpd.read_file(extracted_shp))
        log_metadata(dataset, extracted_shp)
    else:
        print(f"No SHP file found in {dataset}")

print("Cold Path Data Ingestion Complete!")