import geopandas as gpd
import pandas as pd
import duckdb
from shapely import wkt
from shapely.errors import TopologicalError
from pathlib import Path
import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

# === Paths ===
RAW_DIR = Path("storage/delta/raw/administrative_shapefiles/")
TRUSTED_DIR = Path("storage/delta/trusted/structured/administrative_shapefiles/")
TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

# === Utility: Find latest Delta table directory ===
def find_latest_delta_table(base_path: Path):
    """Find the most recent Delta table directory"""
    delta_dirs = []
    for year_month in base_path.iterdir():
        if year_month.is_dir():
            for day_dir in year_month.iterdir():
                if day_dir.is_dir() and any(day_dir.glob("*.parquet")):
                    delta_dirs.append(day_dir)
    
    if not delta_dirs:
        raise FileNotFoundError(f"No Delta tables found under {base_path}")
    
    # Return the most recent directory based on name (YYYYMMDD format)
    return max(delta_dirs, key=lambda x: x.name)

# === Main cleaning logic ===
def clean_admin_boundaries():
    # Initialize Spark Session to read Delta tables
    builder = SparkSession.builder.appName("CleanAdminShapefiles") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    try:
        # Find the latest Delta table directory
        latest_delta_dir = find_latest_delta_table(RAW_DIR)
        print(f"[Admin Boundaries] Reading Delta table from: {latest_delta_dir}")
        
        # Read the entire Delta table
        spark_df = spark.read.format("delta").load(str(latest_delta_dir))
        
        # Convert to Pandas
        df = spark_df.toPandas()
        
        print(f"Read {len(df)} rows from Delta table")
        print(f"Columns: {df.columns.tolist()}")
        if len(df) > 0:
            print(f"Sample data (first row):")
            print(df.iloc[0])

        # Ensure it's a GeoDataFrame with geometry
        if "geometry" not in df.columns and "geometry_wkt" in df.columns:
            print("Converting WKT to geometry...")
            df["geometry"] = df["geometry_wkt"].apply(wkt.loads)
            gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:25831")  # Barcelona's UTM zone
        elif "geometry" in df.columns:
            gdf = gpd.GeoDataFrame(df, geometry="geometry")
        else:
            raise ValueError("No geometry information found in the dataset")

        # Normalize CRS to WGS84
        print("Converting CRS to EPSG:4326...")
        gdf = gdf.to_crs("EPSG:4326")

        # Drop duplicates
        initial_count = len(gdf)
        gdf = gdf.drop_duplicates()
        print(f"Removed {initial_count - len(gdf)} duplicate rows")

        # Repair geometries if invalid
        print("Checking and repairing invalid geometries...")
        invalid_count = (~gdf["geometry"].is_valid).sum()
        if invalid_count > 0:
            print(f"Found {invalid_count} invalid geometries, repairing...")
            gdf["geometry"] = gdf["geometry"].apply(
                lambda geom: geom if geom.is_valid else geom.buffer(0)
            )

        # Convert geometry back to WKT for storage
        gdf["geometry_wkt"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom else None)
        
        # Create a regular DataFrame for DuckDB (without geometry column)
        df_for_duckdb = pd.DataFrame(gdf.drop(columns=["geometry"]))

        # Save to DuckDB
        print("Saving to DuckDB...")
        con = duckdb.connect("bdm_project.duckdb")
        con.execute("INSTALL parquet; LOAD parquet;")
        con.register("admin_boundaries_cleaned", df_for_duckdb)
        con.execute("DROP TABLE IF EXISTS trusted_admin_boundaries")
        con.execute("""
            CREATE TABLE trusted_admin_boundaries AS
            SELECT * FROM admin_boundaries_cleaned
        """)        
        con.close()

        # Save cleaned version to trusted Parquet folder
        output_path = TRUSTED_DIR / f"admin_boundaries_cleaned_{datetime.now().strftime('%Y%m%d')}.parquet"
        df_for_duckdb.to_parquet(output_path, index=False)
        print(f"[Admin Boundaries] Saved cleaned data to: {output_path}")
                    
    finally:
        spark.stop()

# === Run ===
if __name__ == "__main__":
    clean_admin_boundaries()