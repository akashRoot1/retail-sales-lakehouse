# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — cleaned and validated
# MAGIC Types every column, removes duplicates, fees and zero prices, flags cancellations,
# MAGIC computes line revenue, then runs data quality checks. The write only happens if all checks pass.

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from retail_lakehouse.quality import assert_checks, check_silver
from retail_lakehouse.transforms import clean_transactions

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

bronze = spark.table(f"{CATALOG}.{SCHEMA}.bronze_transactions")
silver = clean_transactions(bronze).cache()

bronze_rows, silver_rows = bronze.count(), silver.count()
print(f"Bronze: {bronze_rows:,}  ->  Silver: {silver_rows:,}  "
      f"({bronze_rows - silver_rows:,} rows removed)")

# COMMAND ----------

assert_checks(check_silver(silver, min_rows=1000))

(silver.write.format("delta").mode("overwrite").option("overwriteSchema", True)
 .partitionBy("invoice_month")
 .saveAsTable(f"{CATALOG}.{SCHEMA}.silver_transactions"))
