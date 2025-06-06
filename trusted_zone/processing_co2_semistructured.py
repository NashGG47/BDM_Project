from datetime import datetime
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, from_json, schema_of_json
from pyspark.sql.functions import col, lit, avg
import pytz
import json
import pandas as pd
import os
import json
import glob
from pathlib import Path


def save_parquet_records_as_delta(
    raw_parquet_dir="storage/delta/raw/emissions_data",
    delta_output_dir="trusted_zone/storage/emissions_data/partition_raw"
):

    builder = SparkSession.builder.appName("ExtractAndSaveDelta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    all_records = []

    for fname in os.listdir(raw_parquet_dir):
        if fname.endswith(".snappy.parquet"):
            parquet_path = os.path.join(raw_parquet_dir, fname)
            df = pd.read_parquet(parquet_path)
            for column_name in df.columns:
                cell = df.iloc[0][column_name]
                if isinstance(cell, list): 
                    record = dict(cell)
                    all_records.append(record)

    if all_records:
        df_out = spark.createDataFrame(all_records)
        h_columns = [c for c in df_out.columns if c.startswith('h')]

        if h_columns:
            sum_expr = None
            for c in h_columns:
                if sum_expr is None:
                    sum_expr = col(c).cast("double")
                else:
                    sum_expr += col(c).cast("double")

            avg_expr = sum_expr / len(h_columns)
            df_out = df_out.withColumn("average_contaminant", avg_expr)
        else:
            df_out = df_out.withColumn("average_contaminant", lit(None))
        grouped_df = df_out.groupBy("data", "nom_estacio").agg(
            avg("average_contaminant").alias("average_contaminant")
        )

        from pyspark.sql.functions import first

        additional_cols = ["latitud", "longitud", "contaminant", "unitats",
                           "municipi", "area_urbana", "codi_eoi"]
        agg_exprs = [first(c, ignorenulls=True).alias(c) for c in additional_cols]

        grouped_df = df_out.groupBy("data", "nom_estacio").agg(
            avg("average_contaminant").alias("average_contaminant"),
            *agg_exprs
        )
        output_columns = [
            "data", "latitud", "longitud", "contaminant", "unitats",
            "municipi", "nom_estacio", "area_urbana", "codi_eoi", "average_contaminant"
        ]
        cols_to_keep = [c for c in output_columns if c in grouped_df.columns]
        grouped_df = grouped_df.select(*cols_to_keep)
        for c in cols_to_keep:
            if grouped_df.filter(col(c).isNotNull()).count() == 0:
                grouped_df = grouped_df.drop(c)

        grouped_df.write.format("delta").mode("overwrite").save(delta_output_dir)
        print(f"Saved {grouped_df.count()} grouped records to Delta at {delta_output_dir}")

    else:
        print("No valid records found to save.")

    spark.stop()

save_parquet_records_as_delta()