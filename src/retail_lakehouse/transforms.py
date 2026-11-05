"""Bronze -> Silver -> Gold transformations for the UCI Online Retail II dataset.

All functions are pure DataFrame -> DataFrame so they can be unit-tested locally
and reused unchanged in Databricks notebooks.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Source column -> clean snake_case name
COLUMN_MAP = {
    "Invoice": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_ts",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}

# Stock codes that are fees or adjustments, not products.
NON_PRODUCT_CODES = ["POST", "DOT", "M", "C2", "BANK CHARGES", "AMAZONFEE", "CRUK", "D",
                     "PADS", "ADJUST", "ADJUST2", "TEST001", "TEST002", "S", "B"]

TIMESTAMP_FORMATS = ["yyyy-MM-dd HH:mm:ss", "M/d/yyyy H:mm", "dd/MM/yyyy HH:mm"]


def add_ingestion_metadata(df: DataFrame, source_file: str | None = None) -> DataFrame:
    """Bronze: keep raw columns untouched and record when (and from which file) rows arrived."""
    out = df.withColumn("_ingested_at", F.current_timestamp())
    if "_source_file" not in df.columns:
        out = out.withColumn("_source_file", F.lit(source_file).cast("string"))
    return out


def standardise_columns(df: DataFrame) -> DataFrame:
    """Rename source columns to snake_case; extra columns (e.g. Bronze metadata) are kept."""
    for src, dst in COLUMN_MAP.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)
    return df


def parse_timestamp(col: str) -> F.Column:
    """Try each known date format; returns null if none match (caught by quality checks)."""
    return F.coalesce(*[F.try_to_timestamp(F.col(col), F.lit(fmt)) for fmt in TIMESTAMP_FORMATS])


def clean_transactions(bronze: DataFrame) -> DataFrame:
    """Silver: typed, de-duplicated, product-only transaction lines with revenue."""
    df = standardise_columns(bronze)
    df = (
        df.withColumn("invoice_no", F.trim("invoice_no"))
        .withColumn("stock_code", F.upper(F.trim("stock_code")))
        .withColumn("description", F.initcap(F.trim("description")))
        .withColumn("country", F.trim("country"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("decimal(10,2)"))
        .withColumn("customer_id",
                    F.regexp_replace(F.col("customer_id").cast("string"), r"\.0$", ""))
        .withColumn("invoice_ts", parse_timestamp("invoice_ts"))
    )
    df = (
        df.dropDuplicates(["invoice_no", "stock_code", "description", "quantity",
                           "invoice_ts", "unit_price", "customer_id", "country"])
        .filter(~F.col("stock_code").isin(NON_PRODUCT_CODES))
        .filter(F.col("unit_price") > 0)
        .filter(F.col("quantity") != 0)
        .withColumn("is_cancellation", F.col("invoice_no").startswith("C"))
        .withColumn("line_revenue", (F.col("quantity") * F.col("unit_price")).cast("decimal(14,2)"))
        .withColumn("invoice_date", F.to_date("invoice_ts"))
        .withColumn("invoice_month", F.date_trunc("month", "invoice_ts").cast("date"))
    )
    return df.select(
        "invoice_no", "stock_code", "description", "quantity", "unit_price", "line_revenue",
        "customer_id", "country", "invoice_ts", "invoice_date", "invoice_month",
        "is_cancellation",
    )


def revenue_by_month_country(silver: DataFrame) -> DataFrame:
    """Gold: net revenue (sales minus cancellations) per month and country."""
    return (
        silver.groupBy("invoice_month", "country")
        .agg(
            F.sum("line_revenue").alias("net_revenue"),
            F.sum(F.when(~F.col("is_cancellation"), F.col("line_revenue"))).alias("gross_revenue"),
            F.countDistinct(F.when(~F.col("is_cancellation"), F.col("invoice_no"))).alias("orders"),
            F.countDistinct("customer_id").alias("customers"),
        )
        .fillna(0, subset=["gross_revenue"])
        .orderBy("invoice_month", F.desc("net_revenue"))
    )


def product_performance(silver: DataFrame) -> DataFrame:
    """Gold: units, revenue and return rate per product, ranked by net revenue."""
    agg = silver.groupBy("stock_code").agg(
        F.first("description", ignorenulls=True).alias("description"),
        F.sum(F.when(~F.col("is_cancellation"), F.col("quantity")).otherwise(0)).alias("units_sold"),
        F.sum(F.when(F.col("is_cancellation"), -F.col("quantity")).otherwise(0)).alias("units_returned"),
        F.sum("line_revenue").alias("net_revenue"),
    )
    return (
        agg.withColumn(
            "return_rate",
            F.when(F.col("units_sold") > 0,
                   F.round(F.col("units_returned") / F.col("units_sold"), 4)).otherwise(None),
        )
        .withColumn("revenue_rank", F.dense_rank().over(Window.orderBy(F.desc("net_revenue"))))
        .orderBy("revenue_rank")
    )


def customer_summary(silver: DataFrame) -> DataFrame:
    """Gold: recency, frequency and monetary value per known customer."""
    known = silver.filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    max_date = known.agg(F.max("invoice_date")).collect()[0][0]
    return (
        known.groupBy("customer_id")
        .agg(
            F.first("country", ignorenulls=True).alias("country"),
            F.max("invoice_date").alias("last_purchase_date"),
            F.countDistinct(F.when(~F.col("is_cancellation"), F.col("invoice_no"))).alias("orders"),
            F.sum("line_revenue").alias("net_revenue"),
        )
        .withColumn("recency_days", F.datediff(F.lit(max_date), F.col("last_purchase_date")))
    )
