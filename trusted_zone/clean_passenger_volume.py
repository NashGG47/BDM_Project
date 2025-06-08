import duckdb
import pandas as pd
from pathlib import Path
import os
from datetime import datetime

# Define input/output directories
RAW_BASE = Path("storage/delta/raw/passenger_volume/")
TRUSTED_PATH = Path("storage/delta/trusted/structured/passenger_volume/")
TRUSTED_PATH.mkdir(parents=True, exist_ok=True)

# Step 1: Find the most recent data file
def find_latest_parquet(base_path):
    all_parquet_files = list(base_path.rglob("*.parquet"))
    if not all_parquet_files:
        raise FileNotFoundError(f"No parquet files found under {base_path}")
    latest_file = max(all_parquet_files, key=os.path.getmtime)
    print(f"Found latest file: {latest_file}")
    return latest_file

# Step 2: Load latest partitioned file
latest_file = find_latest_parquet(RAW_BASE)
df = pd.read_parquet(latest_file)

# Step 3: Basic cleaning
df = df.drop_duplicates()
df.columns = [col.lower().strip().replace(" ", "_") for col in df.columns]

# Try converting timestamp column if present
timestamp_candidates = [col for col in df.columns if "timestamp" in col or "fecha" in col or "date" in col]
for col in timestamp_candidates:
    try:
        df[col] = pd.to_datetime(df[col])
        df.rename(columns={col: "timestamp"}, inplace=True)
        break
    except Exception:
        continue

# Drop rows missing essential identifiers
essential_cols = [col for col in df.columns if "station" in col or "timestamp" in col]
df = df.dropna(subset=essential_cols)

# Step 4: Write to DuckDB
con = duckdb.connect("bdm_project.duckdb")
con.execute("INSTALL parquet; LOAD parquet;")
con.register("passenger_volume_cleaned", df)

con.execute("""
    CREATE TABLE IF NOT EXISTS trusted_passenger_volume AS
    SELECT * FROM passenger_volume_cleaned
""")
con.close()

# Step 5: Export cleaned data to trusted zone folder
output_file = TRUSTED_PATH / f"passenger_volume_cleaned_{datetime.now().strftime('%Y%m%d')}.parquet"
df.to_parquet(output_file, index=False)

print(f"Trusted passenger volume data written to: {output_file}")
