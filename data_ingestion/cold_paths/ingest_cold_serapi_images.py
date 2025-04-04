import os
import requests
import json
from serpapi import GoogleSearch
from dotenv import load_dotenv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, regexp_extract
from delta import configure_spark_with_delta_pip
from PIL import Image
from pathlib import Path

# Load environment variables
load_dotenv()
SERPAPI_KEY = os.getenv("SERPAPI_KEY")

# Define base paths (usando pathlib)
BASE_DIR = Path(__file__).resolve().parents[2]
IMAGE_DIR = BASE_DIR / "storage" / "delta" / "raw" / "images" / "serapi" / "images"
DELTA_OUTPUT_PATH = BASE_DIR / "storage" / "delta" / "raw" / "images" / "serapi"
METADATA_PATH = BASE_DIR / "storage" / "delta" / "raw" /  "metadata" / "serapi_metadata.json"
FAIL_LOG_PATH = BASE_DIR / "storage" / "delta" / "raw" /  "metadata" / "serapi_failed_log.json"

IMAGE_DIR.mkdir(parents=True, exist_ok=True)
METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)

# Initialize Spark session with Delta support
builder = SparkSession.builder.appName("IngestSerapiImages") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

def fetch_google_maps_images(query):
    params = {
        "q": query,
        "tbm": "isch",
        "api_key": SERPAPI_KEY
    }
    search = GoogleSearch(params)
    return search.get_dict().get("images_results", [])

def download_image(url, save_path):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            with open(save_path, 'wb') as f:
                f.write(response.content)
            return True
    except Exception as e:
        print(f"Error downloading {url}: {e}")
    return False

def extract_image_metadata(path):
    try:
        with Image.open(path) as img:
            w, h = img.size
        size_kb = os.path.getsize(path) / 1024
        return f"{w}x{h}", round(size_kb, 4)
    except Exception as e:
        print(f"Error extracting metadata from {path}: {e}")
        return None, None

def append_json_entry(entry, path):
    if path.exists():
        try:
            with open(path, "r") as f:
                data = json.load(f)
                if not isinstance(data, list):
                    data = [data]
        except Exception:
            data = []
    else:
        data = []
    data.append(entry)
    with open(path, "w") as f:
        json.dump(data, f, indent=4)

def save_metadata_file(metadata_list):
    new_entry = {
        "dataset_name": "serapi_data",
        "data_count": len(metadata_list),
        "files_metadata": metadata_list,
        "timestamp": datetime.now().isoformat()
    }
    append_json_entry(new_entry, METADATA_PATH)
    print(f"Metadata appended to: {METADATA_PATH}")

def save_failed_log(failed_list):
    new_entry = {
        "fail_count": len(failed_list),
        "failures": failed_list,
        "timestamp": datetime.now().isoformat()
    }
    append_json_entry(new_entry, FAIL_LOG_PATH)
    print(f"Fail log appended to: {FAIL_LOG_PATH}")

def process_and_ingest_image(filename):
    full_path = str(IMAGE_DIR / filename)
    df = spark.read.format("binaryFile") \
        .load(full_path) \
        .withColumn("filename", input_file_name()) \
        .withColumn("id", regexp_extract("filename", r"([^/]+)\.jpg$", 1))

    df.write.format("delta").mode("append").save(str(DELTA_OUTPUT_PATH))
    print(f"Inserted into Delta Lake: {filename}")

def main():
    query = "Barcelona traffic"
    images = fetch_google_maps_images(query)

    metadata_list = []
    failed_images = []

    existing_files = set()
    try:
        existing_files = set(
            spark.read.format("delta")
            .load(str(DELTA_OUTPUT_PATH))
            .select("path")
            .rdd.flatMap(lambda x: x)
            .map(lambda p: os.path.basename(p))
            .collect()
        )
    except Exception:
        print("Delta table not found yet. Will create it.")

    for idx, img in enumerate(images):
        try:
            url = img.get("thumbnail")
            fname = f"serapi_google_maps_image_{idx}.jpg"
            fpath = IMAGE_DIR / fname

            if fname in existing_files:
                print(f"Skipping already inserted: {fname}")
                continue

            if not download_image(url, fpath):
                raise Exception("Failed to download")

            dims, size = extract_image_metadata(fpath)
            if not dims:
                raise Exception("Metadata extraction failed")

            metadata_list.append({
                "file_name": fname,
                "file_size_in_kb": size,
                "image_dimensions": dims,
                "timestamp": datetime.now().isoformat()
            })

            process_and_ingest_image(fname)

        except Exception as e:
            print(f"Error with image {idx}: {e}")
            failed_images.append({
                "index": idx,
                "url": img.get("thumbnail"),
                "error": str(e),
                "timestamp": datetime.now().isoformat()
            })

    if metadata_list:
        save_metadata_file(metadata_list)

    if failed_images:
        save_failed_log(failed_images)

if __name__ == "__main__":
    main()
