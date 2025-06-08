from minio import Minio
from pathlib import Path
import json
import shutil
import matplotlib.pyplot as plt
from processing_unstructured import process_bluesky_posts

# MinIO configuration
client = Minio("localhost:9000", access_key="admin", secret_key="admin123", secure=False)
bucket = "trusted-zone"
base_path = "storage/social_media/bluesky/"

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
images_dir = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky" / "images"
trusted_images_dir = BASE_DIR / "trusted_zone" / "storage" / "processed" / "bluesky" / "images"
metadata_file = BASE_DIR / "trusted_zone" / "storage" / "processed" / "bluesky" / "metadata.json"
failed_log_file = BASE_DIR / "trusted_zone" / "storage" / "processed" / "bluesky" / "failed_log.json"
local_parquet_path = BASE_DIR / "trusted_zone" / "storage" / "processed" / "bluesky" / "posts_clean.parquet"

def generate_metadata(df_spark):
    metadata = []
    for row in df_spark.toLocalIterator():
        metadata.append({
            "uri": row.uri,
            "timestamp": row.timestamp,
            "text": row.text,
            "source": row.source,
            "author_handle": row.author_handle,
            "author_name": row.author_name,
            "author_avatar": row.author_avatar,
            "likes": row.likes,
            "reposts": row.reposts,
            "replies": row.replies,
            "image_url": row.image_url,
            "image_path": row.image_path,
            "image_width": row.image_width,
            "image_height": row.image_height,
            "image_size_kb": row.image_size_kb
        })
    return metadata

def upload_to_minio(local_path, object_path):
    client.fput_object(bucket, object_path, str(local_path))
    print(f"Uploaded to MinIO: {object_path}")

def main():
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    # Get SparkSession and cleaned DataFrame from processing
    spark, df_clean = process_bluesky_posts()

    # Count original and cleaned records
    df_raw = spark.read.format("delta").load(str(BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky"))
    total_count = df_raw.count()
    cleaned_count = df_clean.count()
    removed_count = total_count - cleaned_count

    print(f"\n📊 Total original records: {total_count}")
    print(f"❌ Removed records (duplicates/nulls): {removed_count}")
    print(f"✅ Cleaned records saved: {cleaned_count}\n")

    # Save the cleaned data as a single Parquet file
    local_parquet_path.mkdir(parents=True, exist_ok=True)
    df_clean.coalesce(1).write.mode("overwrite").parquet(str(local_parquet_path))

    # Find the generated .parquet file
    actual_parquet = next(local_parquet_path.glob("*.parquet"))

    # Generate metadata
    metadata = generate_metadata(df_clean)
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    with open(failed_log_file, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    # Upload files to MinIO
    upload_to_minio(actual_parquet, base_path + "posts_clean.parquet")
    upload_to_minio(metadata_file, base_path + "metadata.json")
    upload_to_minio(failed_log_file, base_path + "failed_log.json")

    # Copy images from landing to trusted zone
    trusted_images_dir.mkdir(parents=True, exist_ok=True)
    for image in images_dir.glob("*.jpg"):
        try:
            target = trusted_images_dir / image.name
            shutil.copy(image, target)
        except Exception as e:
            print(f"Failed to copy image {image.name}: {e}")

    # Upload images from trusted zone to MinIO
    for image in trusted_images_dir.glob("*.jpg"):
        try:
            upload_to_minio(image, base_path + "images/" + image.name)
        except Exception as e:
            print(f"Failed to upload image {image.name}: {e}")

    # 📈 Generate summary chart
    labels = ['Total', 'Removed (duplicates/nulls)', 'Saved in Minio']
    values = [total_count, removed_count, cleaned_count]

    plt.figure(figsize=(6, 4))
    plt.bar(labels, values)
    plt.title("Post Processing Summary - Trusted Zone")
    plt.ylabel("Number of Records")
    plt.tight_layout()
    plt.savefig("summary_chart.png")
    print("📊 Summary chart saved as summary_chart.png")
    spark.stop()

if __name__ == "__main__":
    main()
    
