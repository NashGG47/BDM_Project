
from minio import Minio
from pathlib import Path
import json
import shutil
from processing_unstructured import process_bluesky_posts

# Config MinIO
client = Minio("localhost:9000", access_key="admin", secret_key="admin123", secure=False)
bucket = "trusted-zone"
base_path = "storage/social_media/bluesky/"

# Paths
BASE_DIR = Path(__file__).resolve().parents[1]
images_dir = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky" / "images"  # landing zone
trusted_images_dir = BASE_DIR / "trusted_zone" / "storage" / "processed" / "bluesky" / "images"  # trusted zone
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

    # Obtener SparkSession y DataFrame limpio desde processing
    spark, df_clean = process_bluesky_posts()

    # Guardar el Parquet como un solo archivo dentro de una carpeta
    local_parquet_path.mkdir(parents=True, exist_ok=True)
    df_clean.coalesce(1).write.mode("overwrite").parquet(str(local_parquet_path))

    # Buscar el archivo .parquet generado dentro de la carpeta
    actual_parquet = next(local_parquet_path.glob("*.parquet"))

    # Generar metadata
    metadata = generate_metadata(df_clean)
    metadata_file.parent.mkdir(parents=True, exist_ok=True)
    with open(metadata_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)

    with open(failed_log_file, "w", encoding="utf-8") as f:
        json.dump([], f, indent=2)

    # Subir archivos a MinIO
    upload_to_minio(actual_parquet, base_path + "posts_clean.parquet")
    upload_to_minio(metadata_file, base_path + "metadata.json")
    upload_to_minio(failed_log_file, base_path + "failed_log.json")

    # Copiar imágenes desde landing a Trusted Zone
    trusted_images_dir.mkdir(parents=True, exist_ok=True)
    for image in images_dir.glob("*.jpg"):
        try:
            target = trusted_images_dir / image.name
            shutil.copy(image, target)
        except Exception as e:
            print(f"Failed to copy image {image.name}: {e}")

    # Subir imágenes desde Trusted Zone a MinIO
    for image in trusted_images_dir.glob("*.jpg"):
        try:
            upload_to_minio(image, base_path + "images/" + image.name)
        except Exception as e:
            print(f"Failed to upload image {image.name}: {e}")

if __name__ == "__main__":
    main()