"""End-to-end run: landing -> bronze -> silver -> gold.

Usage::

    python -m lakehouse.pipeline --root data              # Parquet, local
    python -m lakehouse.pipeline --root data --format delta
"""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lakehouse import bronze, gold, quality, silver
from lakehouse.config import LakehouseConfig
from lakehouse.io import overwrite_table, read_table
from lakehouse.spark import get_spark


def run(spark: SparkSession, cfg: LakehouseConfig) -> None:
    fmt = cfg.table_format

    # ---------------------------------------------------------------- bronze
    print("Bronze")
    for source in ("suppliers", "purchase_orders"):
        new = bronze.ingest(spark, cfg.landing / source, cfg.table("bronze", source), source, fmt)
        print(f"  {source}: {len(new)} new batch(es) {new}")

    # --------------------------------------------------- silver: suppliers SCD2
    print("Silver")
    bronze_suppliers = read_table(spark, cfg.table("bronze", "suppliers"), fmt)
    history_path = cfg.table("silver", "supplier_history")
    history = read_table(spark, history_path, fmt)

    all_batches = sorted(r["_batch_id"] for r in bronze_suppliers.select("_batch_id").distinct().collect())
    if history is not None:
        # Only apply batches newer than what the history already reflects (safe to re-run).
        # Format inside Spark so the session time zone (UTC) applies.
        last = history.agg(F.date_format(F.max("valid_from"), "yyyy-MM-dd")).first()[0]
        pending = [b for b in all_batches if b > last]
    else:
        pending = all_batches
    print(f"  supplier_history: applying batch(es) {pending}")

    if pending:
        history = silver.historize_suppliers(history, bronze_suppliers, pending)
        overwrite_table(history, history_path, fmt)
        history = read_table(spark, history_path, fmt)

    quality.enforce(
        [
            quality.scd2_one_current_per_key(history, silver.SUPPLIER_KEYS),
            quality.scd2_no_overlaps(history, silver.SUPPLIER_KEYS),
        ]
    )

    # ------------------------------------------- silver: purchase orders + quarantine
    orders = silver.clean_purchase_orders(read_table(spark, cfg.table("bronze", "purchase_orders"), fmt))
    latest = silver.latest_per_order(orders)
    valid, invalid = silver.split_valid_invalid(latest, history)
    overwrite_table(valid, cfg.table("silver", "purchase_orders"), fmt)
    overwrite_table(invalid, cfg.table("quarantine", "purchase_orders"), fmt)
    valid = read_table(spark, cfg.table("silver", "purchase_orders"), fmt)
    print(f"  purchase_orders: {valid.count()} valid, {invalid.count()} quarantined")

    quality.enforce(
        [
            quality.unique_key(valid, ["po_id"], "purchase_orders"),
            quality.not_null(valid, ["po_id", "supplier_id", "order_date", "amount"], "purchase_orders"),
        ]
    )

    # ------------------------------------------------------------------ gold
    print("Gold")
    dim = gold.dim_supplier_current(history)
    spend = gold.fact_spend_monthly(valid, history)
    overwrite_table(dim, cfg.table("gold", "dim_supplier_current"), fmt)
    overwrite_table(spend, cfg.table("gold", "fact_spend_monthly"), fmt)
    print(f"  dim_supplier_current: {dim.count()} rows")
    print(f"  fact_spend_monthly: {spend.count()} rows")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--root", type=Path, default=Path("data"), help="folder containing landing/")
    parser.add_argument("--format", choices=["parquet", "delta"], default="parquet")
    args = parser.parse_args()

    cfg = LakehouseConfig(root=args.root.resolve(), table_format=args.format)
    spark = get_spark(table_format=cfg.table_format)
    try:
        run(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
