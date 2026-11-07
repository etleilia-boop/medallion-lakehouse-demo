"""Silver -> Gold: business-ready tables."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def dim_supplier_current(supplier_history: DataFrame) -> DataFrame:
    """Live suppliers only: the current, non-deleted version of each key."""
    return supplier_history.filter(F.col("is_current") & ~F.col("is_deleted")).select(
        "supplier_id",
        "supplier_name",
        "country",
        "category",
        "payment_terms_days",
        "is_active",
        F.col("valid_from").alias("version_since"),
    )


def fact_spend_monthly(orders: DataFrame, supplier_history: DataFrame) -> DataFrame:
    """Monthly spend by supplier category *as it was when the order was placed*.

    This is a point-in-time join: each order is matched to the supplier version whose
    validity window contains the order date. Using the current version instead would
    silently restate history every time a supplier is re-categorised.
    """
    o = orders.alias("o")
    h = supplier_history.alias("h")
    order_ts = F.col("o.order_date").cast("timestamp")
    joined = o.join(
        h,
        (F.col("o.supplier_id") == F.col("h.supplier_id"))
        & (order_ts >= F.col("h.valid_from"))
        & (order_ts < F.col("h.valid_to")),
        "left",
    )
    return (
        joined.groupBy(
            F.date_format("o.order_date", "yyyy-MM").alias("month"),
            F.coalesce(F.col("h.category"), F.lit("Unknown")).alias("category"),
            F.col("o.currency").alias("currency"),
        )
        .agg(F.sum("o.amount").alias("total_spend"), F.countDistinct("o.po_id").alias("order_count"))
        .orderBy("month", "category")
    )
