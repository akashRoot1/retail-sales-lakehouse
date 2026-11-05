from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from retail_lakehouse.quality import DataQualityError, assert_checks, check_silver
from retail_lakehouse.transforms import (
    add_ingestion_metadata,
    clean_transactions,
    customer_summary,
    product_performance,
    revenue_by_month_country,
)

COLUMNS = ["Invoice", "StockCode", "Description", "Quantity", "InvoiceDate", "Price",
           "Customer ID", "Country"]

RAW_ROWS = [
    # normal sales
    ("489434", "85048", "15CM CHRISTMAS GLASS BALL", "12", "2009-12-01 07:45:00", "6.95", "13085.0", "United Kingdom"),
    ("489434", "79323P", "PINK CHERRY LIGHTS", "12", "2009-12-01 07:45:00", "6.75", "13085.0", "United Kingdom"),
    ("489435", "85048", "15CM CHRISTMAS GLASS BALL", "6", "12/2/2009 9:10", "6.95", "12682.0", "France"),
    # exact duplicate of the first line -> removed
    ("489434", "85048", "15CM CHRISTMAS GLASS BALL", "12", "2009-12-01 07:45:00", "6.95", "13085.0", "United Kingdom"),
    # cancellation (return)
    ("C489449", "85048", "15CM CHRISTMAS GLASS BALL", "-2", "2009-12-01 10:33:00", "6.95", "13085.0", "United Kingdom"),
    # postage fee -> removed
    ("489435", "POST", "POSTAGE", "1", "12/2/2009 9:10", "18.00", "12682.0", "France"),
    # zero price -> removed
    ("489436", "21523", "DOORMAT", "1", "2010-01-05 11:00:00", "0.00", "", "United Kingdom"),
    # no customer id (kept for revenue, excluded from customer summary)
    ("489437", "79323P", "PINK CHERRY LIGHTS", "1", "2010-01-05 11:00:00", "6.75", "", "United Kingdom"),
]


@pytest.fixture(scope="session")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("tests")
               .config("spark.sql.shuffle.partitions", "1")
               .config("spark.ui.enabled", "false").getOrCreate())
    yield session
    session.stop()


@pytest.fixture
def silver(spark):
    bronze = add_ingestion_metadata(spark.createDataFrame(RAW_ROWS, COLUMNS), "test.csv")
    return clean_transactions(bronze)


def test_bronze_keeps_raw_columns_and_adds_metadata(spark):
    bronze = add_ingestion_metadata(spark.createDataFrame(RAW_ROWS, COLUMNS), "test.csv")
    assert set(COLUMNS) <= set(bronze.columns)
    assert bronze.select("_source_file").first()[0] == "test.csv"
    assert bronze.count() == len(RAW_ROWS)


def test_silver_removes_duplicates_fees_and_zero_prices(silver):
    assert silver.count() == 5
    codes = {r.stock_code for r in silver.collect()}
    assert "POST" not in codes and "21523" not in codes


def test_silver_parses_both_date_formats(silver):
    assert silver.filter("invoice_ts IS NULL").count() == 0
    france = silver.filter("country = 'France'").first()
    assert str(france.invoice_date) == "2009-12-02"


def test_silver_flags_cancellations_and_computes_revenue(silver):
    ret = silver.filter("is_cancellation").first()
    assert ret.invoice_no == "C489449" and ret.line_revenue == Decimal("-13.90")
    first = silver.filter("invoice_no = '489434' AND stock_code = '85048'").first()
    assert first.line_revenue == Decimal("83.40") and first.customer_id == "13085"


def test_silver_passes_quality_checks(silver):
    assert_checks(check_silver(silver))


def test_quality_checks_catch_bad_data(spark, silver):
    broken = silver.unionByName(silver.limit(1))  # inject a duplicate
    with pytest.raises(DataQualityError):
        assert_checks(check_silver(broken))


def test_gold_revenue_nets_off_cancellations(silver):
    uk_dec = (revenue_by_month_country(silver)
              .filter("country = 'United Kingdom' AND invoice_month = '2009-12-01'").first())
    assert uk_dec.gross_revenue == Decimal("164.40")   # 83.40 + 81.00
    assert uk_dec.net_revenue == Decimal("150.50")     # minus 13.90 return
    assert uk_dec.orders == 1


def test_gold_product_performance_ranks_and_return_rate(silver):
    top = product_performance(silver).first()
    assert top.stock_code == "85048" and top.revenue_rank == 1
    assert top.units_sold == 18 and top.units_returned == 2
    assert top.return_rate == pytest.approx(0.1111, abs=1e-4)


def test_gold_customer_summary_excludes_unknown_customers(silver):
    customers = {r.customer_id: r for r in customer_summary(silver).collect()}
    assert set(customers) == {"13085", "12682"}
    assert customers["13085"].orders == 1
