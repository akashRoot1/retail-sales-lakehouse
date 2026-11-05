# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze — raw ingestion
# MAGIC Loads the raw CSV exactly as received (all columns as strings) into a Delta table,
# MAGIC adding the ingestion time and source file so every row can be traced back.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from pyspark.sql import functions as F

from retail_lakehouse.transforms import add_ingestion_metadata

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
RAW_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw/"

# COMMAND ----------

raw = (
    spark.read.option("header", True).option("inferSchema", False)
    .csv(RAW_PATH)
    .select("*", F.col("_metadata.file_path").alias("_source_file"))
)
bronze = add_ingestion_metadata(raw)

(bronze.write.format("delta").mode("overwrite").option("overwriteSchema", True)
 .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_transactions"))

print(f"Bronze rows: {bronze.count():,}")
