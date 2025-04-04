import os
import json
import time
import requests
import zipfile
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import geopandas as gpd
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark Session with Delta Support
builder = SparkSession.builder.appName("AdminShapefilesIngestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

BASE_DIR = "data_ingestion/cold_paths/"
DELTA_TABLE_PATH = "storage/delta/raw/administrative_shapefiles/"
METADATA_FILE = "storage/delta/raw/metadata/administrative_shapefiles.json"

cold_path_datasets = {
    "administrative_shapefiles": "https://opendata-ajuntament.barcelona.cat/data/dataset/808daafa-d9ce-48c0-925a-fa5afdb1ed41/resource/11851135-6919-4dcb-91ed-821e5e87a428/download"
}

def download_and_save(dataset_name, url, file_format):
    year_month = datetime.now().strftime('%Y%m')  # e.g. "202504"
    local_path = os.path.join(BASE_DIR, dataset_name, year_month, f"raw_{datetime.now().strftime('%Y-%m-%d')}.{file_format}")
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    print(f"Downloading {dataset_name} from {url}...")
    response = requests.get(url)
    with open(local_path, "wb") as file:
        file.write(response.content)
    print(f"Saved: {local_path}")
    return local_path

def extract_shapefile(zip_path, extract_to):
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)
    for file in os.listdir(extract_to):
        if file.endswith(".shp"):
            return os.path.join(extract_to, file)
    return None

def store_in_delta(dataset_name, gdf):
    print(f"Storing {dataset_name} in Delta Lake...")

    # Convert the geometry column to WKT (Well-Known Text)
    gdf["geometry_wkt"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom else None)
    df = gdf.drop(columns=["geometry"])  # Remove the original geometry column

    # Debug: Print column data types before conversion
    print("Column Data Types Before Conversion:")
    print(df.dtypes)

    df = df.astype(str)

    current_ym = datetime.now().strftime('%Y%m')   
    current_ymd = datetime.now().strftime('%Y%m%d')  
    final_delta_path = os.path.join(DELTA_TABLE_PATH, current_ym, current_ymd)
    os.makedirs(final_delta_path, exist_ok=True)

    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").save(final_delta_path)
    print(f"Delta Table Updated: {final_delta_path}")

    row_count = len(df)
    return final_delta_path, row_count

def log_metadata(dataset_name, raw_file_path, source_url, file_size, delta_path, row_count, processing_duration):
    os.makedirs(os.path.dirname(METADATA_FILE), exist_ok=True) 

    log_entry = {
        "dataset": dataset_name,
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

    print(f"Metadata logged for {dataset_name}")

# Main Execution
if __name__ == "__main__":
    dataset = "administrative_shapefiles"
    source_url = cold_path_datasets[dataset]
    
    start_time = time.time()
    zip_file = download_and_save(dataset, source_url, "zip")
    
    file_size = os.path.getsize(zip_file)
    
    extraction_folder = os.path.dirname(zip_file)
    extracted_shp = extract_shapefile(zip_file, extraction_folder)
    
    if extracted_shp:
        gdf = gpd.read_file(extracted_shp)
        delta_path, row_count = store_in_delta(dataset, gdf)
        processing_duration = time.time() - start_time
        log_metadata(dataset, extracted_shp, source_url, file_size, delta_path, row_count, processing_duration)
    else:
        print(f"No SHP file found in {dataset}")

    print("Cold Path Data Ingestion Complete!")
