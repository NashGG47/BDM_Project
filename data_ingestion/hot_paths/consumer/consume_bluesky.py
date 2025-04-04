# consume_bluesky.py

import os
import json
import base64
import requests
from pathlib import Path
from io import BytesIO
from PIL import Image
from datetime import datetime
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

BASE_DIR = Path(__file__).resolve().parents[3]
IMAGE_DIR = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky" / "images"
PARQUET_DIR = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky"
METADATA_FILE = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "bluesky_metadata.json"
FAILED_LOG_FILE = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "bluesky_failed_log.json"

IMAGE_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
METADATA_FILE.parent.mkdir(parents=True, exist_ok=True)

builder = SparkSession.builder.appName("BlueskyConsumer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

def download_image(url, local_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
        return response.content
    except Exception as e:
        print(f"Error downloading image: {url} | {e}")
        return None

def extract_image_metadata(img_bytes):
    try:
        img = Image.open(BytesIO(img_bytes))
        width, height = img.size
        size_kb = len(img_bytes) / 1024
        return width, height, size_kb
    except Exception as e:
        print(f"Error extracting image metadata | {e}")
        return None, None, None

def main():
    consumer = KafkaConsumer(
        'bluesky_posts',
        bootstrap_servers='localhost:9092',
        group_id='bluesky_consumer_group_v3',
        enable_auto_commit=True,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    metadata, failures, records = [], [], []

    # Check for existing duplicates
    try:
        existing_df = spark.read.format("delta").load(str(PARQUET_DIR)).select("uri").distinct()
        existing_uris = set(row.uri for row in existing_df.collect())
    except:
        existing_uris = set()

    for msg in consumer:
        try:
            post = msg.value

            timestamp = post.get("timestamp")
            uri = post.get("uri")

            if uri in existing_uris:
                failures.append({"uri": uri, "error": "Duplicate post"})
                continue

            text = post.get("text")
            source = post.get("source")
            media_items = post.get("media_urls", [])
            media_items = [media for media in media_items if media]  # Filter out None or empty items
            author = post.get("author", {})
            metrics = post.get("metrics", {})
            post_did = uri.split("/")[2] if uri else "unknown"

            if not media_items:
                record = {
                    "uri": uri,
                    "timestamp": timestamp,
                    "text": text,
                    "source": source,
                    "author_handle": author.get("handle"),
                    "author_name": author.get("displayName"),
                    "author_avatar": author.get("avatar"),
                    "likes": metrics.get("likes"),
                    "reposts": metrics.get("reposts"),
                    "replies": metrics.get("replies"),
                    "image_url": None,
                    "image_path": None,
                    "image_width": None,
                    "image_height": None,
                    "image_size_kb": None,
                    "image_binary": None
                }
                metadata.append(record)
                records.append(record)

            for idx, media in enumerate(media_items):
                if isinstance(media, dict) and media.get("$type") == "blob":
                    cid = media.get("ref", {}).get("$link")
                    if not cid:
                        failures.append({"uri": uri, "error": "CID not found in blob"})
                        continue
                    image_url = f"https://bsky.social/xrpc/com.atproto.sync.getBlob?did={post_did}&cid={cid}"
                elif isinstance(media, str):
                    image_url = media
                else:
                    failures.append({"uri": uri, "error": f"Unrecognized media format: {media}"})
                    continue

                filename = f"{source}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{idx}.jpg"
                image_path = IMAGE_DIR / filename
                image_bytes = download_image(image_url, image_path)

                if not image_bytes:
                    failures.append({"uri": uri, "error": f"Could not download image {image_url}"})
                    continue

                width, height, size_kb = extract_image_metadata(image_bytes)

                record = {
                    "uri": uri,
                    "timestamp": timestamp,
                    "text": text,
                    "source": source,
                    "author_handle": author.get("handle"),
                    "author_name": author.get("displayName"),
                    "author_avatar": author.get("avatar"),
                    "likes": metrics.get("likes"),
                    "reposts": metrics.get("reposts"),
                    "replies": metrics.get("replies"),
                    "image_url": image_url,
                    "image_path": str(image_path.relative_to(BASE_DIR)),
                    "image_width": width,
                    "image_height": height,
                    "image_size_kb": size_kb,
                    "image_binary": image_bytes
                }
                metadata.append({"timestamp": timestamp,
                                 "source": source,
                                 "image_url": record.get("image_url"),
                                 "image_path": record.get("image_path"),
                                 "image_width": record.get("image_width"),
                                 "image_height": record.get("image_height"),
                                 "image_size_kb": record.get("image_size_kb")
                                })
                records.append(record)

            if records:
                schema = StructType([
                    StructField("uri", StringType()),
                    StructField("timestamp", StringType()),
                    StructField("text", StringType()),
                    StructField("source", StringType()),
                    StructField("author_handle", StringType()),
                    StructField("author_name", StringType()),
                    StructField("author_avatar", StringType()),
                    StructField("likes", IntegerType()),
                    StructField("reposts", IntegerType()),
                    StructField("replies", IntegerType()),
                    StructField("image_url", StringType()),
                    StructField("image_path", StringType()),
                    StructField("image_width", IntegerType()),
                    StructField("image_height", IntegerType()),
                    StructField("image_size_kb", FloatType()),
                    StructField("image_binary", BinaryType())
                ])

                df = spark.createDataFrame(records, schema)
                df.write.format("delta").mode("append").save(str(PARQUET_DIR))

        except Exception as e:
            failures.append({"uri": uri if 'uri' in locals() else None, "error": str(e)})
        
        finally:

            # Always try to write results (metadata and failures)
            try:
                # Save metadata cumulatively
                if METADATA_FILE.exists() and METADATA_FILE.stat().st_size > 0:
                    with open(METADATA_FILE, "r", encoding="utf-8") as f:
                        try:
                            existing_metadata = json.load(f)
                        except json.JSONDecodeError:
                            existing_metadata = []
                else:
                    existing_metadata = []

                existing_metadata.extend(metadata)
                with open(METADATA_FILE, "w", encoding="utf-8") as f:
                    json.dump(existing_metadata, f, indent=2, ensure_ascii=False)

                # Save errors cumulatively
                if failures:
                    if FAILED_LOG_FILE.exists() and FAILED_LOG_FILE.stat().st_size > 0:
                        with open(FAILED_LOG_FILE, "r", encoding="utf-8") as f:
                            try:
                                existing_failures = json.load(f)
                            except json.JSONDecodeError:
                                existing_failures = []
                    else:
                        existing_failures = []

                    existing_failures.extend(failures)
                    with open(FAILED_LOG_FILE, "w", encoding="utf-8") as f:
                        json.dump(existing_failures, f, indent=2, ensure_ascii=False)
                
                # Always clearly notify when something is saved:
                if records or metadata or failures:
                    print(f"Saved {len(records)} posts, {len(metadata)} metadata entries, {len(failures)} errors to Delta Lake.")

                records.clear()
                metadata.clear()
                failures.clear()

            except Exception as file_exception:
                print(f"Critical error saving JSON files: {file_exception}")


if __name__ == "__main__":
    main()
