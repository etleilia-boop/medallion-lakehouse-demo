"""Landing -> Bronze.

Bronze keeps the source data as-is (every column as string) and adds lineage columns.
Ingestion is idempotent: a landing file already present in bronze is skipped.
"""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from lakehouse.io import append_table, read_table

# Source contracts: expected columns, all read as strings. Typing happens in silver.
SOURCE_COLUMNS: dict[str, list[str]] = {
    "suppliers": ["supplier_id", "supplier_name", "country", "category", "payment_terms_days", "is_active"],
    "purchase_orders": ["po_id", "supplier_id", "order_date", "amount", "currency"],
}

_BATCH_RE = re.compile(r"_(\d{4}-\d{2}(?:-\d{2})?)\.csv$")


def batch_id_from_filename(name: str) -> str:
    """``suppliers_2026-02-01.csv`` -> ``2026-02-01``; ``purchase_orders_2026-02.csv`` -> ``2026-02``."""
    match = _BATCH_RE.search(name)
    if not match:
        raise ValueError(f"Cannot derive batch id from file name: {name}")
    return match.group(1)


def _already_ingested(spark: SparkSession, bronze_path: str, table_format: str) -> set[str]:
    existing = read_table(spark, bronze_path, table_format)
    if existing is None:
        return set()
    return {r["_source_file"] for r in existing.select("_source_file").distinct().collect()}


def read_landing_file(spark: SparkSession, path: Path, source: str) -> DataFrame:
    schema = StructType([StructField(c, StringType(), True) for c in SOURCE_COLUMNS[source]])
    df = (
        spark.read.option("header", "true")
        .option("enforceSchema", "false")  # fail if the header does not match the contract
        .option("mode", "FAILFAST")
        .schema(schema)
        .csv(str(path))
    )
    return (
        df.withColumn("_source_file", F.lit(path.name))
        .withColumn("_batch_id", F.lit(batch_id_from_filename(path.name)))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def ingest(spark: SparkSession, landing_dir: Path, bronze_path: str, source: str, table_format: str) -> list[str]:
    """Append new landing files for ``source`` to bronze. Returns the batch ids ingested."""
    done = _already_ingested(spark, bronze_path, table_format)
    new_files = sorted(p for p in landing_dir.glob(f"{source}_*.csv") if p.name not in done)

    batches = []
    for path in new_files:
        append_table(read_landing_file(spark, path, source), bronze_path, table_format, partition_by=["_batch_id"])
        batches.append(batch_id_from_filename(path.name))
    return batches
