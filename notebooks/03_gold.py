# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Gold — business-ready tables
# MAGIC Three tables for reporting (Power BI connects to these):
# MAGIC - `gold_revenue_by_month_country` — net revenue, orders and customers
# MAGIC - `gold_product_performance` — units, returns, return rate and revenue rank
# MAGIC - `gold_customer_summary` — recency, frequency and monetary value per customer

# COMMAND ----------

import os
import sys

sys.path.append(os.path.abspath("../src"))
from retail_lakehouse.transforms import (
    customer_summary,
    product_performance,
    revenue_by_month_country,
)

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "retail")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

# COMMAND ----------

silver = spark.table(f"{CATALOG}.{SCHEMA}.silver_transactions")

gold_tables = {
    "gold_revenue_by_month_country": revenue_by_month_country(silver),
    "gold_product_performance": product_performance(silver),
    "gold_customer_summary": customer_summary(silver),
}

for name, df in gold_tables.items():
    (df.write.format("delta").mode("overwrite").option("overwriteSchema", True)
     .saveAsTable(f"{CATALOG}.{SCHEMA}.{name}"))
    print(f"{name}: {df.count():,} rows")

# COMMAND ----------

display(spark.table(f"{CATALOG}.{SCHEMA}.gold_revenue_by_month_country").limit(20))
