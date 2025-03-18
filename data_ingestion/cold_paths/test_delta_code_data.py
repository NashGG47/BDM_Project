from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# Initialize Spark Session with Delta Support
builder = SparkSession.builder \
    .appName("TestCodePathData") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Define Delta Table Path for Code Path Data
delta_table_path = "data_ingestion/delta_storage/cold_paths/administrative_shapefiles"

# Test Loading the Delta Table
try:
    df = spark.read.format("delta").load(delta_table_path)
    print("Successfully loaded Delta table!")
    # Print the schema
    df.printSchema()

    # Show limited rows with truncation for better readability
    df.show(5, truncate=50)

    # Show only key columns with limited rows
    df.select("DISTRICTE", "geometry_wkt").show(5, truncate=50)

    # Count the number of records in the dataset
    print(f"Total records in dataset: {df.count()}")

except Exception as e:
    print(f"Error loading Delta table: {e}")

# Stop Spark Session
spark.stop()
