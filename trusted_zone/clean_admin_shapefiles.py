import geopandas as gpd
import pandas as pd
import duckdb
from shapely import wkt
from shapely.errors import TopologicalError
from pathlib import Path
import os
from datetime import datetime

# === Paths ===
RAW_DIR = Path("storage/delta/raw/administrative_shapefiles/")
TRUSTED_DIR = Path("storage/delta/trusted/structured/administrative_shapefiles/")
TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

# === Utility: Find latest partitioned Parquet ===
def find_latest_parquet(base_dir: Path):
    parquet_files = list(base_dir.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found under {base_dir}")
    return max(parquet_files, key=os.path.getmtime)

# === Main cleaning logic ===
def clean_admin_boundaries():
    latest_file = find_latest_parquet(RAW_DIR)
    print(f"[Admin Boundaries] Reading: {latest_file}")
    df = pd.read_parquet(latest_file)

    # Ensure it's a GeoDataFrame with geometry (loaded from WKT if needed)
    if "geometry" not in df.columns and "geometry_wkt" in df.columns:
        df["geometry"] = df["geometry_wkt"].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    elif "geometry" in df.columns:
        gdf = gpd.GeoDataFrame(df, geometry="geometry")
    else:
        raise ValueError("No geometry information found in the dataset")

    # Normalize CRS
    gdf = gdf.to_crs("EPSG:4326")

    # Drop duplicates
    gdf = gdf.drop_duplicates()

    # Repair geometries if invalid
    gdf["geometry"] = gdf["geometry"].apply(
        lambda geom: geom if geom.is_valid else geom.buffer(0)
    )

    # Convert geometry to WKT for compatibility with tabular storage
    gdf["geometry_wkt"] = gdf["geometry"].apply(lambda geom: geom.wkt if geom else None)
    gdf = gdf.drop(columns=["geometry"])

    # Save to DuckDB
    con = duckdb.connect("bdm_project.duckdb")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.register("admin_boundaries_cleaned", gdf)
    con.execute("""
        CREATE TABLE IF NOT EXISTS trusted_admin_boundaries AS
        SELECT * FROM admin_boundaries_cleaned
    """)
    con.close()

    # Save cleaned version to trusted Parquet folder
    output_path = TRUSTED_DIR / f"admin_boundaries_cleaned_{datetime.now().strftime('%Y%m%d')}.parquet"
    gdf.to_parquet(output_path, index=False)
    print(f"[Admin Boundaries] ✅ Saved cleaned data to: {output_path}")

# === Run ===
if __name__ == "__main__":
    clean_admin_boundaries()
