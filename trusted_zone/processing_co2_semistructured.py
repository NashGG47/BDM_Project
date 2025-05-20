from datetime import datetime
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
import pytz
import json
import ast  # safer alternative to eval

def process_emissions_data(
    delta_path="storage/delta/raw/emissions_data",
    sample_limit=1000,
    allowed_areas=["urban", "suburban", "rural"]
):
    # Set up Spark session with Delta support
    builder = SparkSession.builder \
        .appName("ProcessEmissionsData") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Read raw delta file
    df = spark.read.format("delta").load(delta_path)

    # --- Step 1: Infer JSON schema from column "0" (first row only) ---
    sample_raw = df.selectExpr("CAST(`0` AS STRING)").limit(1).collect()[0][0]

    # Try to parse the sample string safely into a Python dict
    try:
        # Try parsing as JSON directly first
        sample_dict = json.loads(sample_raw)
    except json.JSONDecodeError:
        try:
            # If not JSON, try safely parse Python literal dict format (like single quotes etc.)
            sample_dict = ast.literal_eval(sample_raw)
        except Exception as e:
            # If both fail, fallback to raw string and warn user
            print(f"Warning: Could not parse sample row as JSON or dict: {e}")
            sample_dict = sample_raw  # this may cause schema inference to fail

    # Convert dict back to JSON string for schema inference
    if isinstance(sample_dict, dict):
        sample_json_str = json.dumps(sample_dict)
    else:
        sample_json_str = sample_dict  # may still be raw string

    inferred_schema = schema_of_json(sample_json_str)

    # --- Step 2: Parse full dataset using the inferred schema ---
    df = df.withColumn("parsed", from_json(col("0").cast("string"), inferred_schema))
    df = df.select("parsed.*")  # unpack to top-level fields

    # --- Step 3: Filter valid and allowed rows ---
    df = df.filter(
        col("data").isNotNull() &
        col("latitud").isNotNull() &
        col("longitud").isNotNull()
    )
    df = df.filter(col("area_urbana").isin(allowed_areas))

    # --- Step 4: Select required columns ---
    hour_cols = [f"h{str(i).zfill(2)}" for i in range(1, 25)]
    melted = df.select(
        "data", "latitud", "longitud", "contaminant", "unitats",
        "municipi", "nom_estacio", "area_urbana", "codi_eoi",
        *hour_cols
    )

    # --- Step 5: Convert rows to list of records ---
    emissions_records = []
    rows = melted.limit(sample_limit).collect()

    for row in rows:
        base_time = datetime.strptime(row['data'], "%Y-%m-%dT%H:%M:%S.%f")
        lat = float(row['latitud'])
        lon = float(row['longitud'])

        for h in range(1, 25):
            value = row[f"h{str(h).zfill(2)}"]
            if value is None or value == "":
                continue
            hour_time = base_time.replace(hour=(h-1), minute=0, second=0)
            hour_time = hour_time.astimezone(pytz.utc)
            emissions_records.append({
                "measurement": "air_quality",
                "tags": {
                    "station": row["nom_estacio"],
                    "municipality": row["municipi"],
                    "contaminant": row["contaminant"],
                    "unit": row["unitats"],
                    "area": row["area_urbana"],
                    "eoi_code": row["codi_eoi"]
                },
                "fields": {
                    "value": float(value),
                    "lat": lat,
                    "lon": lon
                },
                "time": hour_time.isoformat()
            })

    return emissions_records
