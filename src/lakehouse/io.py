"""Format-agnostic table read/write helpers."""

from __future__ import annotations

import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession


def table_exists(path: str) -> bool:
    p = Path(path)
    return p.exists() and any(p.iterdir())


def read_table(spark: SparkSession, path: str, table_format: str) -> DataFrame | None:
    if not table_exists(path):
        return None
    return spark.read.format(table_format).load(path)


def overwrite_table(df: DataFrame, path: str, table_format: str, partition_by: list[str] | None = None) -> None:
    """Overwrite a table, even when ``df`` was derived from that same table.

    Delta handles this natively (snapshot isolation). Plain Parquet does not: Spark would
    delete the files it is still reading. For Parquet we write to a staging folder first
    and then swap it in. The swap uses the local filesystem, which is fine for local runs
    and CI; on a real lake use Delta.
    """
    writer_opts = {"partitionBy": partition_by} if partition_by else {}

    if table_format == "delta":
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(path, **writer_opts)
        return

    staging = f"{path}__staging"
    shutil.rmtree(staging, ignore_errors=True)
    df.write.format(table_format).mode("overwrite").save(staging, **writer_opts)
    shutil.rmtree(path, ignore_errors=True)
    Path(staging).rename(path)


def append_table(df: DataFrame, path: str, table_format: str, partition_by: list[str] | None = None) -> None:
    writer = df.write.format(table_format).mode("append")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)
