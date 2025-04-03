import os
import json
import requests
import pandas as pd
from PIL import Image
from datetime import datetime
from urllib.parse import urlencode
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
API_KEY = os.getenv("API_GOOGLE_MAPS")

# Paths and configuration
BASE_DIR = Path(__file__).resolve().parents[2]
IMAGE_DIR = BASE_DIR / "storage" / "delta" / "raw" / "images" / "google_maps" / "images"
DELTA_OUTPUT_PATH = BASE_DIR / "storage" / "delta" / "raw" / "images" / "google_maps"
METADATA_PATH = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "google_maps_metadata.json"
FAIL_LOG_PATH = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "google_maps_failed_log.json"
ROUTES_CSV = BASE_DIR / "data_ingestion" / "cold_paths" / "google_maps_paths.csv"

IMAGE_DIR.mkdir(parents=True, exist_ok=True)
METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)

# Spark session
builder = SparkSession.builder.appName("IngestGoogleMapsImages") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "12")  # optimized partitioning
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Helper functions
def download_image(url, save_path):
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return True
    except Exception as e:
        print(f"Error downloading image: {e}")
    return False

def extract_image_metadata(path):
    try:
        with Image.open(path) as img:
            w, h = img.size
        size_kb = os.path.getsize(path) / 1024
        return f"{w}x{h}", round(size_kb, 4)
    except Exception as e:
        print(f"Metadata extraction error: {e}")
        return None, None

def append_json_entry(entry, path):
    if path.exists():
        try:
            with open(path, "r") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = [data]
        except:
            data = []
    else:
        data = []
    data.append(entry)
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def delta_table_exists(path):
    return DeltaTable.isDeltaTable(spark, str(path))

def route_has_changed(name, duration_traffic, color):
    if not delta_table_exists(DELTA_OUTPUT_PATH):
        return True
    existing = spark.read.format("delta").load(str(DELTA_OUTPUT_PATH))
    filtered = existing.filter(existing["id"].contains(name)).toPandas()
    if filtered.empty:
        return True
    last = filtered.sort_values(by="file_name", ascending=False).iloc[0]
    return last["congestion_level"] != color or last["duration_in_traffic_sec"] != duration_traffic

def save_metadata_file(metadata_list):
    entry = {
        "dataset_name": "google_maps_traffic_routes",
        "data_count": len(metadata_list),
        "files_metadata": metadata_list,
        "timestamp": datetime.now().isoformat()
    }
    append_json_entry(entry, METADATA_PATH)
    print(f"Metadata appended to: {METADATA_PATH}")

def save_failed_log(failed_list):
    entry = {
        "fail_count": len(failed_list),
        "failures": failed_list,
        "timestamp": datetime.now().isoformat()
    }
    append_json_entry(entry, FAIL_LOG_PATH)
    print(f"Fail log appended to: {FAIL_LOG_PATH}")

# Main logic
def main():
    try:
        routes_df = pd.read_csv(ROUTES_CSV)
    except Exception as e:
        print(f"Could not read CSV: {e}")
        return

    metadata_list = []
    failed_images = []
    enriched_data = []

    for index, row in routes_df.iterrows():
        name = row["Ruta"].replace(" ", "_").replace("/", "_")
        lat1, lon1, lat2, lon2 = row["Lat1"], row["Lon1"], row["Lat2"], row["Lon2"]

        print(f"Processing route: {name}")

        # Call Directions API
        directions_url = "https://maps.googleapis.com/maps/api/directions/json"
        params = {
            "origin": f"{lat1},{lon1}",
            "destination": f"{lat2},{lon2}",
            "departure_time": "now",
            "key": API_KEY
        }
        response = requests.get(directions_url, params=params)
        data = response.json()

        if data.get("status") != "OK":
            print(f"API error for {name}: {data.get('status')}")
            failed_images.append({"route": name, "error": data.get("status")})
            continue

        try:
            route = data["routes"][0]
            polyline = route["overview_polyline"]["points"]
            leg = route["legs"][0]
            duration = leg["duration"]["value"]
            duration_traffic = leg.get("duration_in_traffic", leg["duration"])["value"]

            ratio = duration_traffic / duration
            color = "red" if ratio > 1.3 else "orange" if ratio > 1.1 else "green"

            if not route_has_changed(name, duration_traffic, color):
                print(f"Skipping unchanged route: {name}")
                continue

            # Build Static Maps API URL
            static_map_url = "https://maps.googleapis.com/maps/api/staticmap?"
            map_params = {
                "size": "640x400",
                "scale": 2,
                "format": "png",
                "maptype": "roadmap",
                "path": f"color:{color}|weight:5|enc:{polyline}",
                "key": API_KEY,
            }
            full_url = static_map_url + urlencode(map_params)
            filename = f"{index:02d}_{name}_route.png"
            full_path = IMAGE_DIR / filename

            if not download_image(full_url, full_path):
                raise Exception("Image download failed")

            dims, size_kb = extract_image_metadata(full_path)
            if not dims:
                raise Exception("Metadata extraction failed")

            metadata = {
                "id": filename.replace(".png", ""),
                "file_name": filename,
                "image_index": index,
                "file_size_in_kb": size_kb,
                "image_dimensions": dims,
                "route_name": name,
                "origin": f"{lat1},{lon1}",
                "destination": f"{lat2},{lon2}",
                "duration_sec": duration,
                "duration_in_traffic_sec": duration_traffic,
                "congestion_level": color,
                "timestamp": datetime.now().isoformat(),
                "static_map_url": full_url,
                "local_path": str(full_path)
            }

            json_metadata = {
                "id": filename.replace(".png", ""),
                "file_name": filename,
                "image_index": index,
                "file_size_in_kb": size_kb,
                "image_dimensions": dims,
                "timestamp": datetime.now().isoformat(),
                "static_map_url": full_url,
                "local_path": str(full_path)
            }

            metadata_list.append(json_metadata)
            enriched_data.append(metadata)

        except Exception as e:
            print(f"Error on route {name}: {e}")
            failed_images.append({"route": name, "error": str(e)})

    # Delta Lake ingestion
    if enriched_data:
        images_df = spark.read.format("binaryFile").load([str(m["local_path"]) for m in enriched_data])
        metadata_df = spark.createDataFrame(enriched_data).drop("local_path")

        final_df = images_df.withColumn("id", regexp_extract(input_file_name(), r"([^/]+)\.png$", 1)) \
                            .join(metadata_df, on="id", how="inner")

        final_df.write.format("delta").mode("append").save(str(DELTA_OUTPUT_PATH))
        print(f"Ingested {final_df.count()} images into Delta Lake")

    if metadata_list:
        save_metadata_file(metadata_list)

    if failed_images:
        save_failed_log(failed_images)

if __name__ == "__main__":
    main()
