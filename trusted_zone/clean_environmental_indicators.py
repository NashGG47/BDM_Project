import duckdb
import pandas as pd
from pathlib import Path
import os
from datetime import datetime
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def find_latest_delta_table(base_path: Path):
    """Find the most recent Delta table directory"""
    # Delta tables are organized by year/month/day
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

def clean_environmental_indicators():
    RAW_DIR = Path("storage/delta/raw/environmental_indicators/")
    TRUSTED_DIR = Path("storage/delta/trusted/structured/environmental_indicators/")
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

    # Initialize Spark Session to read Delta tables
    builder = SparkSession.builder.appName("CleanEnvironmentalIndicators") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    try:
        # Find the latest Delta table directory
        latest_delta_dir = find_latest_delta_table(RAW_DIR)
        print(f"[Environmental Indicators] Reading Delta table from: {latest_delta_dir}")
        
        # Read the entire Delta table, not just one parquet file
        spark_df = spark.read.format("delta").load(str(latest_delta_dir))
        
        # Convert to Pandas for cleaning
        df = spark_df.toPandas()
        
        print(f"Read {len(df)} rows from Delta table")
        print(f"Columns: {df.columns.tolist()}")
        print(f"Sample data:\n{df.head()}")

        df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_").str.replace(r"[;{}()\\n\\t=]", "", regex=True)

        column_mapping = {
            "año": "year",
            "year": "year",  # In case it's already mapped
            "gwh_traccion_electrica": "gwh_traccion_electrica",
            "millones_litros_diesel_consumidos": "millones_litros_diesel_consumidos",
            "gwh_l_diesel": "gwh_l_diesel",
            "gwh_total": "gwh_total",
            "intensidad_energetica_wh_ut": "intensidad_energetica_wh_ut",
            "intensidad_carbono_g_co2_ut": "intensidad_carbono_g_co2_ut",
            "gastos_inversiones_ambientales_euros": "gastos_inversiones_ambientales_euros",
            "consumo_agua_m3": "consumo_agua_m3",
            "generacion_residuos_peligrosos_toneladas": "generacion_residuos_peligrosos_toneladas",
            "porcentaje_trafico_viajeros_trenes_baja_emision_acustica": "porcentaje_trafico_viajeros_baja_emision",
            "porcentaje_trafico_mercancias_trenes_baja_emision_acustica": "porcentaje_trafico_mercancias_baja_emision"
        }
        
        # Only rename if the key exists in columns
        rename_dict = {k: v for k, v in column_mapping.items() if k in df.columns}
        if rename_dict:
            df.rename(columns=rename_dict, inplace=True)

        # Data quality checks
        df = df.drop_duplicates()
        df = df.dropna(subset=["year"])
        df['year'] = df['year'].astype(int)
        
        # Sort by year for better organization
        df = df.sort_values('year', ascending=False)
        
        print(f"After cleaning: {len(df)} rows")

        # Store in DuckDB
        con = duckdb.connect("bdm_project.duckdb")
        con.execute("INSTALL parquet; LOAD parquet;")
        con.register("env_indicators_cleaned", df)
        con.execute("DROP TABLE IF EXISTS trusted_environmental_indicators")
        con.execute("CREATE TABLE trusted_environmental_indicators AS SELECT * FROM env_indicators_cleaned")
        
        con.close()

        # Save to parquet in trusted zone
        output_path = TRUSTED_DIR / f"environmental_indicators_cleaned_{datetime.now().strftime('%Y%m%d')}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"[Environmental Indicators] Saved cleaned data to: {output_path}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    clean_environmental_indicators()