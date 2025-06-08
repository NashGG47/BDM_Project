from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lower, trim
from pathlib import Path

# Base path
BASE_DIR = Path(__file__).resolve().parents[1]
LANDING_DIR = BASE_DIR / "storage" / "delta" / "raw" / "social_media" / "bluesky"

def process_bluesky_posts():
    builder = SparkSession.builder \
        .appName("BlueskyProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    print("📥 Reading from landing zone...")
    df = spark.read.format("delta").load(str(LANDING_DIR))

    df_clean = df.dropDuplicates(["uri"]) \
        .filter(col("text").isNotNull()) \
        .filter(col("uri").isNotNull() & col("timestamp").isNotNull() & col("source").isNotNull()) \
        .withColumn("text", lower(trim(col("text"))))

    print(f"✅ Cleaned records: {df_clean.count()}")
    return spark, df_clean
