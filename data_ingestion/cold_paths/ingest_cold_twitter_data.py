import os
import json
import time
import requests
from pathlib import Path
from io import BytesIO
from PIL import Image
from datetime import datetime
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from delta import configure_spark_with_delta_pip

# ======================= CONFIGURACIÓN =========================
BASE_DIR = Path(__file__).resolve().parents[2]
RAW_IMAGE_DIR = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "twitter" / "images"
PARQUET_DIR = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "twitter"
METADATA_PATH = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "twitter_metadata.json"
FAILED_LOG_PATH = BASE_DIR / "storage" / "delta" / "raw" / "metadata" / "twitter_failed_log.json"

RAW_IMAGE_DIR.mkdir(parents=True, exist_ok=True)
PARQUET_DIR.mkdir(parents=True, exist_ok=True)
METADATA_PATH.parent.mkdir(parents=True, exist_ok=True)

load_dotenv()
TWITTER_BEARER_TOKEN = os.getenv("TWITTER_BEARER_TOKEN")

MAX_RESULTS = 10
TWITTER_ACCOUNTS = ["transit", "DGTes"]
QUERY = " OR ".join([f"from:{acct}" for acct in TWITTER_ACCOUNTS])

HEADERS = {
    "Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"
}

TWEET_FIELDS = "created_at,lang,public_metrics,possibly_sensitive,attachments"
MEDIA_FIELDS = "url,type"
EXPANSIONS = "attachments.media_keys,author_id"
USER_FIELDS = "username"

# ======================= SPARK SESSION =========================
builder = SparkSession.builder.appName("IngestTwitterData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.shuffle.partitions", "12")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

# ======================= FUNCIONES =============================
def fetch_tweets():
    url = "https://api.twitter.com/2/tweets/search/recent"
    params = {
        "query": QUERY,
        "max_results": MAX_RESULTS,
        "tweet.fields": TWEET_FIELDS,
        "media.fields": MEDIA_FIELDS,
        "expansions": EXPANSIONS,
        "user.fields": USER_FIELDS
    }
    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching tweets: {response.status_code} - {response.text}")
        return None

def download_image(url, local_path):
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(local_path, "wb") as f:
            f.write(response.content)
        return response.content
    except Exception as e:
        print(f"Failed downloading image {url} — {e}")
        return None

def extract_image_metadata(img_bytes):
    try:
        img = Image.open(BytesIO(img_bytes))
        width, height = img.size
        size_kb = len(img_bytes) / 1024
        return width, height, size_kb
    except Exception as e:
        print(f"Failed extracting image metadata — {e}")
        return None, None, None

def build_tweet_row(tweet, user_map, media_map):
    tweet_id = tweet["id"]
    text = tweet.get("text", "")
    author_id = tweet["author_id"]
    username = user_map.get(author_id, "")
    created_at = tweet.get("created_at")
    lang = tweet.get("lang")
    sensitive = tweet.get("possibly_sensitive", False)
    metrics = tweet.get("public_metrics", {})
    likes = metrics.get("like_count", 0)
    retweets = metrics.get("retweet_count", 0)

    # Imagen asociada (si hay)
    media_keys = tweet.get("attachments", {}).get("media_keys", [])
    image_binary = None
    image_url = None
    image_path = None
    width = None
    height = None
    size_kb = None

    for key in media_keys:
        media_info = media_map.get(key)
        if media_info and media_info.get("type") == "photo":
            image_url = media_info["url"]
            filename = f"{tweet_id}_{key}.jpg"
            image_path = RAW_IMAGE_DIR / filename
            image_bytes = download_image(image_url, image_path)
            if image_bytes:
                image_binary = image_bytes
                width, height, size_kb = extract_image_metadata(image_bytes)
            break

    # Construir registro
    return {
        "tweet_id": tweet_id,
        "username": username,
        "author_id": author_id,
        "text": text,
        "created_at": created_at,
        "lang": lang,
        "sensitive": sensitive,
        "likes": likes,
        "retweets": retweets,
        "image_url": image_url,
        "image_path": str(image_path.relative_to(BASE_DIR)) if image_path else None,
        "image_width": width,
        "image_height": height,
        "image_size_kb": size_kb,
        "image_binary": image_binary
    }

# ======================= MAIN ================================
def main():
    data = fetch_tweets()
    if not data or "data" not in data:
        print("No data retrieved from Twitter API.")
        return

    users = {u["id"]: u["username"] for u in data.get("includes", {}).get("users", [])}
    media = {m["media_key"]: {"url": m["url"], "type": m["type"]}
             for m in data.get("includes", {}).get("media", []) if m.get("type") == "photo"}

    tweets = data["data"]
    processed = []
    metadata_list = []
    failed = []

    for tweet in tweets:
        try:
            row = build_tweet_row(tweet, users, media)
            processed.append(row)
            metadata_list.append({
                "tweet_id": row["tweet_id"],
                "username": row["username"],
                "created_at": row["created_at"],
                "image_url": row["image_url"],
                "image_path": row["image_path"],
                "image_width": row["image_width"],
                "image_height": row["image_height"],
                "image_size_kb": row["image_size_kb"]
            })
        except Exception as e:
            failed.append({
                "tweet": tweet,
                "error": str(e)
            })

    if processed:
        schema = StructType([
            StructField("tweet_id", StringType(), False),
            StructField("username", StringType(), True),
            StructField("author_id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("sensitive", BooleanType(), True),
            StructField("likes", IntegerType(), True),
            StructField("retweets", IntegerType(), True),
            StructField("image_url", StringType(), True),
            StructField("image_path", StringType(), True),
            StructField("image_width", IntegerType(), True),
            StructField("image_height", IntegerType(), True),
            StructField("image_size_kb", FloatType(), True),
            StructField("image_binary", BinaryType(), True)
        ])
        df = spark.createDataFrame(processed, schema=schema)
        df.write.format("delta").mode("append").save(str(PARQUET_DIR))
        print(f"Guardado {len(processed)} registros en Delta Lake")

    # if metadata_list:
    #     with open(METADATA_PATH, "w", encoding="utf-8") as f:
    #         json.dump(metadata_list, f, indent=2, ensure_ascii=False)

    # if failed:
    #     with open(FAILED_LOG_PATH, "w", encoding="utf-8") as f:
    #         json.dump(failed, f, indent=2, ensure_ascii=False)
    #     print(f"Guardado log de errores en {FAILED_LOG_PATH}")

    #Guardar metadata acumulativa
    if metadata_list:
        existing_metadata = []
        if METADATA_PATH.exists():
            with open(METADATA_PATH, "r", encoding="utf-8") as f:
                try:
                    existing_metadata = json.load(f)
                except json.JSONDecodeError:
                    existing_metadata = []
        combined = existing_metadata + metadata_list
        with open(METADATA_PATH, "w", encoding="utf-8") as f:
            json.dump(combined, f, indent=2, ensure_ascii=False)

    #Guardar fallos acumulativos
    if failed:
        existing_failed = []
        if FAILED_LOG_PATH.exists():
            with open(FAILED_LOG_PATH, "r", encoding="utf-8") as f:
                try:
                    existing_failed = json.load(f)
                except json.JSONDecodeError:
                    existing_failed = []
        combined_failed = existing_failed + failed
        with open(FAILED_LOG_PATH, "w", encoding="utf-8") as f:
            json.dump(combined_failed, f, indent=2, ensure_ascii=False)
        print(f"Guardado log de errores en {FAILED_LOG_PATH}")

if __name__ == "__main__":
    main()
