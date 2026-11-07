"""SparkSession factory.

Locally and in CI we use plain Parquet so tests start fast and need no extra jars.
Pass ``table_format="delta"`` to enable Delta Lake (requires the ``delta`` extra).
"""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark(app_name: str = "medallion-lakehouse", table_format: str = "parquet") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
        # Keep partition values (e.g. _batch_id=2026-01-01) as strings, not dates.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
    )
    if table_format == "delta":
        from delta import configure_spark_with_delta_pip

        builder = builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension").config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        builder = configure_spark_with_delta_pip(builder)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
