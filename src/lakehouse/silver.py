"""Bronze -> Silver: typing, cleansing, deduplication, SCD2 and quarantine."""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from lakehouse.scd2 import apply_scd2

SUPPLIER_KEYS = ["supplier_id"]
SUPPLIER_TRACKED = ["supplier_name", "country", "category", "payment_terms_days", "is_active"]


# --------------------------------------------------------------------------- suppliers
def clean_suppliers(bronze_batch: DataFrame) -> DataFrame:
    """Type and normalise one supplier snapshot; drop exact duplicates per key."""
    typed = bronze_batch.select(
        F.upper(F.trim("supplier_id")).alias("supplier_id"),
        F.trim("supplier_name").alias("supplier_name"),
        F.upper(F.trim("country")).alias("country"),
        F.trim("category").alias("category"),
        F.col("payment_terms_days").cast("int").alias("payment_terms_days"),
        F.lower(F.trim("is_active")).isin("true", "1", "yes").alias("is_active"),
        "_ingested_at",
    ).filter(F.col("supplier_id").isNotNull())

    # If the source sends the same key twice, keep one row deterministically.
    w = Window.partitionBy("supplier_id").orderBy(F.col("_ingested_at").desc(), *[F.col(c) for c in SUPPLIER_TRACKED])
    return typed.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn", "_ingested_at")


def batch_effective_ts(batch_id: str) -> datetime:
    """Snapshot batches are named by date; the date is the effective timestamp."""
    return datetime.strptime(batch_id, "%Y-%m-%d")


def historize_suppliers(history: DataFrame | None, bronze: DataFrame, batch_ids: list[str]) -> DataFrame | None:
    """Apply snapshot batches to the supplier history, oldest first."""
    for batch_id in sorted(batch_ids):
        snapshot = clean_suppliers(bronze.filter(F.col("_batch_id") == batch_id))
        history = apply_scd2(history, snapshot, SUPPLIER_KEYS, SUPPLIER_TRACKED, batch_effective_ts(batch_id))
    return history


# --------------------------------------------------------------------- purchase orders
def clean_purchase_orders(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.upper(F.trim("po_id")).alias("po_id"),
        F.upper(F.trim("supplier_id")).alias("supplier_id"),
        F.to_date("order_date", "yyyy-MM-dd").alias("order_date"),
        F.col("amount").cast("decimal(18,2)").alias("amount"),
        F.upper(F.trim("currency")).alias("currency"),
        "_batch_id",
        "_source_file",
        "_ingested_at",
    )


def split_valid_invalid(orders: DataFrame, supplier_history: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Route rows that break a rule to quarantine, with the reason, instead of dropping them."""
    known = supplier_history.select("supplier_id").distinct().withColumn("_known", F.lit(True))
    checked = (
        orders.join(known, "supplier_id", "left")
        .withColumn(
            "_dq_reason",
            F.when(F.col("po_id").isNull(), "missing po_id")
            .when(F.col("order_date").isNull(), "invalid order_date")
            .when(F.col("amount").isNull(), "missing or invalid amount")
            .when(F.col("amount") <= 0, "non-positive amount")
            .when(F.col("_known").isNull(), "unknown supplier_id"),
        )
        .drop("_known")
    )

    valid = checked.filter(F.col("_dq_reason").isNull()).drop("_dq_reason")
    invalid = checked.filter(F.col("_dq_reason").isNotNull())
    return valid, invalid


def latest_per_order(orders: DataFrame) -> DataFrame:
    """A PO can be re-sent in a later batch (correction). The latest batch wins."""
    w = Window.partitionBy("po_id").orderBy(F.col("_batch_id").desc(), F.col("_ingested_at").desc())
    return orders.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")
