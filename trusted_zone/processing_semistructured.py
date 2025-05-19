#steps to organize and preprocess the data
import json
import xml.etree.ElementTree as ET
from datetime import datetime
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, unix_timestamp, when
from pyspark.sql.functions import date_format

# 1. Validate Data fields, so the co2 levels, the timestamps make sure everyhting is in the right format
# 2. Normalize units and format
# 3. Filter in gencat geographical scope
"""
La red viaria de Barcelona cuenta con numerosas rutas que permiten desplazarse de forma rápida y segura. Algunas de las más relevantes son:

C-31: Vía esencial que une la ciudad con la costa y el aeropuerto.
B-10: Circunvalación urbana que agiliza el tráfico en Barcelona.
N-II: Ruta nacional que comunica la provincia con Girona y Madrid.
C-16: Carretera que conecta Barcelona con el Vallès y los Pirineos, con tramos de autopista de peaje.
Red de autopistas y autovías más transitadas en Barcelona
Barcelona dispone de autopistas y autovías que optimizan los trayectos tanto dentro de la provincia como hacia otras regiones. Algunas de las más importantes son:

AP-7: Principal eje del Corredor Mediterráneo, conectando Barcelona con Valencia y Francia.
A-2: Autovía que une la capital catalana con Madrid.
B-23: Acceso rápido desde Barcelona a la AP-7 y al Baix Llobregat.
C-58: Autovía que facilita los desplazamientos entre Barcelona, Sabadell y Terrassa.
C-32: Autopista que enlaza Barcelona con la Costa del Maresme y el Garraf.
B-20: Ronda urbana que permite evitar el tráfico del centro de la ciudad.

"""
# 4. Clean missing or erroneous data
# 5. Convert to InfluxDB format


def process_gencat_incidents(
    delta_path="storage/delta/raw/gencat_incidents/2025-05",
    sample_limit=1000
):
    """
    Loads, cleans, filters, and processes Gencat traffic incident data from Delta Lake,
    returning a list of dicts formatted for InfluxDB ingestion.

    Parameters:
        delta_path (str): Path to the Delta Lake data.
        sample_limit (int): Number of records to collect and convert.

    Returns:
        List[dict]: List of processed incident records formatted for InfluxDB.
    """
    # --- Step 1: Start Spark session with Delta support ---
    builder = SparkSession.builder \
        .appName("ReadDeltaGencat") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # --- Step 2: Load Delta Lake data ---
    df = spark.read.format("delta").load(delta_path)

    # --- Step 3: Validate data ---
    df = df.filter(col("timestamp").isNotNull() & col("latitude").isNotNull() & col("longitude").isNotNull())

    # --- Step 4: Filter geographical scope ---
    barcelona_keywords = ["C-31", "B-10", "N-II", "C-16", "AP-7", "A-2", "B-23", "C-58", "C-32", "B-20", "B-30"]
    geo_filter = " OR ".join([f"road LIKE '%{r}%'" for r in barcelona_keywords])
    df = df.filter(geo_filter)

    # --- Step 5: Drop missing `road` or `cause` ---
    df = df.filter(col("road").isNotNull() & col("cause").isNotNull())

    # --- Step 6: Feature engineering ---
    df = df.withColumn("datetime", col("timestamp").cast("timestamp"))
    df = df.withColumn("day_of_week", date_format(col("datetime"), "EEEE"))
    df = df.withColumn("is_weekend", when(col("day_of_week").isin("Saturday", "Sunday"), 1).otherwise(0))
    df = df.withColumn("road_category", when(col("road").startswith("A"), "Major")
                                        .when(col("road").startswith("AP"), "Major")
                                        .otherwise("Minor"))

    # --- Step 7: Convert rows to InfluxDB format ---
    def convert_row(row):
        timestamp = row['timestamp']   
        return {
            "measurement": "traffic_incidents",
            "tags": {
                "incident_id": row['id'],
                "road": row['road'],
                "cause": row['cause'],
                "direction": row['direction']
            },
            "fields": {
                "pk_start": row['pk_start'],
                "pk_end": row['pk_end'],
                "description": row['description'],
                "lat": row['latitude'],
                "lon": row['longitude'],
                "timestamp": timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
            }
        }

    records = df.limit(sample_limit).collect()
    processed_data = [convert_row(row.asDict()) for row in records]

    #df.select("road", "datetime", "is_weekend", "road_category").show(5, truncate=False)

    return processed_data

