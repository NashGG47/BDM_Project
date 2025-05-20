import duckdb
import pandas as pd
from pathlib import Path
import os
from datetime import datetime

def find_latest_parquet(base_path: Path):
    files = list(base_path.rglob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files found under {base_path}")
    return max(files, key=os.path.getmtime)

def clean_environmental_indicators():
    RAW_DIR = Path("storage/delta/raw/environmental_indicators/")
    TRUSTED_DIR = Path("storage/delta/trusted/structured/environmental_indicators/")
    TRUSTED_DIR.mkdir(parents=True, exist_ok=True)

    latest_file = find_latest_parquet(RAW_DIR)
    print(f"[Environmental Indicators] Reading: {latest_file}")
    df = pd.read_parquet(latest_file)

    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_").str.replace(r"[;{}()\\n\\t=]", "", regex=True)

    column_mapping = {
        "año": "year",
        "gwh_traccion_electrica": "gwh_traccion_electrica",
        "millones_de_litros_de_diesel_consumidos": "millones_litros_diesel_consumidos",
        "gwh_l_diesel": "gwh_l_diesel",
        "gwh_total": "gwh_total",
        "intensidad_energetica_wh_ut": "intensidad_energetica_wh_ut",
        "intensidad_de_carbono_g_co2_ut": "intensidad_carbono_g_co2_ut",
        "gastos_e_inversiones_ambientales_miles_de_euros": "gastos_inversiones_ambientales_euros",
        "consumo_de_agua_m3": "consumo_agua_m3",
        "generacion_de_residuos_peligrosos_toneladas": "generacion_residuos_peligrosos_toneladas",
        "porcentaje_trafico_viajeros_con_trenes_baja_emision_acustica": "porcentaje_trafico_viajeros_baja_emision",
        "porcentaje_trafico_mercancias_con_trenes_baja_emision_acustica": "porcentaje_trafico_mercancias_baja_emision"
    }
    df.rename(columns=column_mapping, inplace=True)

    df = df.drop_duplicates()
    df = df.dropna(subset=["year"])

    con = duckdb.connect("bdm_project.duckdb")
    con.execute("INSTALL parquet; LOAD parquet;")
    con.register("env_indicators_cleaned", df)
    con.execute("CREATE TABLE IF NOT EXISTS trusted_environmental_indicators AS SELECT * FROM env_indicators_cleaned")
    con.close()

    output_path = TRUSTED_DIR / f"environmental_indicators_cleaned_{datetime.now().strftime('%Y%m%d')}.parquet"
    df.to_parquet(output_path, index=False)
    print(f"[Environmental Indicators] ✅ Saved cleaned data to: {output_path}")

if __name__ == "__main__":
    clean_environmental_indicators()
