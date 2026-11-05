"""Data quality checks that gate each layer of the lakehouse."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class DataQualityError(ValueError):
    """Raised when a layer fails its checks, stopping the pipeline."""


@dataclass
class CheckResult:
    name: str
    passed: bool
    detail: str


def check_silver(silver: DataFrame, min_rows: int = 1) -> list[CheckResult]:
    """Checks every Silver load must pass before Gold is built."""
    total = silver.count()
    nulls = silver.agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(c)
          for c in ["invoice_no", "stock_code", "quantity", "unit_price", "invoice_ts", "country"]]
    ).collect()[0].asDict()
    dupes = total - silver.dropDuplicates(
        ["invoice_no", "stock_code", "quantity", "invoice_ts", "unit_price", "customer_id"]
    ).count()
    bad_price = silver.filter(F.col("unit_price") <= 0).count()
    bad_sign = silver.filter(
        (F.col("is_cancellation") & (F.col("quantity") > 0))
        | (~F.col("is_cancellation") & (F.col("quantity") < 0))
    ).count()

    return [
        CheckResult("row_count", total >= min_rows, f"{total} rows (min {min_rows})"),
        CheckResult("no_nulls_in_key_columns", all(v == 0 for v in nulls.values()),
                    ", ".join(f"{k}={v}" for k, v in nulls.items() if v) or "none"),
        CheckResult("no_duplicate_lines", dupes == 0, f"{dupes} duplicates"),
        CheckResult("positive_prices", bad_price == 0, f"{bad_price} rows with price <= 0"),
        CheckResult("quantity_sign_matches_cancellation", bad_sign == 0,
                    f"{bad_sign} rows where sign and cancellation flag disagree"),
    ]


def assert_checks(results: list[CheckResult]) -> None:
    """Print a report and raise if any check failed."""
    for r in results:
        print(f"[{'PASS' if r.passed else 'FAIL'}] {r.name}: {r.detail}")
    failed = [r.name for r in results if not r.passed]
    if failed:
        raise DataQualityError(f"Data quality checks failed: {failed}")
