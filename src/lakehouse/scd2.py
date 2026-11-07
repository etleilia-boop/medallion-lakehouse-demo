"""Slowly Changing Dimension Type 2, as a pure DataFrame transformation.

``apply_scd2(history, snapshot, ...)`` returns the full new history. Keeping it pure
(no I/O) makes it easy to unit-test and to reuse with Parquet or Delta.

History columns added on top of the business columns:

* ``row_hash``   - SHA-256 of the tracked columns, used to detect changes
* ``valid_from`` - when this version became effective (inclusive)
* ``valid_to``   - when it stopped being effective (exclusive); HIGH_DATE if open
* ``is_current`` - exactly one current row per key
* ``is_deleted`` - tombstone: the key disappeared from the full snapshot

Rules for each key, comparing the current version (C) with the snapshot (S):

==================  =====================================================
C and S, same hash  keep C as-is (no new version: re-runs are idempotent)
C and S, new hash   close C, open a new version from S
only S              open a first version
only C (live)       close C, open a tombstone (is_deleted = true)
only C (tombstone)  keep as-is
C tombstone, S      close tombstone, open a new live version (reactivation)
==================  =====================================================
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from lakehouse.config import HIGH_DATE

SCD_COLUMNS = ["row_hash", "valid_from", "valid_to", "is_current", "is_deleted"]


def row_hash(cols: list[str]) -> F.Column:
    # Null-safe and separator-safe: "a|" + "b" must not equal "a" + "|b".
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("\u0000NULL")) for c in cols]
    return F.sha2(F.concat_ws("\u001f", *parts), 256)


def _open_version(df: DataFrame, effective_ts: F.Column, deleted: bool) -> DataFrame:
    return (
        df.withColumn("valid_from", effective_ts)
        .withColumn("valid_to", F.to_timestamp(F.lit(HIGH_DATE)))
        .withColumn("is_current", F.lit(True))
        .withColumn("is_deleted", F.lit(deleted))
    )


def apply_scd2(
    history: DataFrame | None,
    snapshot: DataFrame,
    keys: list[str],
    tracked: list[str],
    effective_ts: datetime,
) -> DataFrame:
    """Merge a full ``snapshot`` into ``history`` effective at ``effective_ts``."""
    business_cols = keys + tracked
    # Pass as a string so Spark interprets it in the session time zone (UTC), not the
    # Python process's local zone.
    eff = F.to_timestamp(F.lit(effective_ts.strftime("%Y-%m-%d %H:%M:%S")))
    snap = snapshot.select(*business_cols).withColumn("row_hash", row_hash(tracked))

    if history is None:
        return _open_version(snap, eff, deleted=False).select(*business_cols, *SCD_COLUMNS)

    history = history.select(*business_cols, *SCD_COLUMNS)
    closed = history.filter(~F.col("is_current"))
    current = history.filter(F.col("is_current"))

    c = current.alias("c")
    s = snap.alias("s")
    joined = c.join(s, on=keys, how="full_outer")

    c_exists = F.col("c.row_hash").isNotNull()
    s_exists = F.col("s.row_hash").isNotNull()
    c_deleted = F.coalesce(F.col("c.is_deleted"), F.lit(False))

    unchanged = c_exists & s_exists & (F.col("c.row_hash") == F.col("s.row_hash")) & ~c_deleted
    changed = c_exists & s_exists & ((F.col("c.row_hash") != F.col("s.row_hash")) | c_deleted)
    new_key = ~c_exists & s_exists
    removed = c_exists & ~s_exists & ~c_deleted
    still_removed = c_exists & ~s_exists & c_deleted

    def current_cols() -> list[F.Column]:
        return [F.col(k) for k in keys] + [F.col(f"c.{x}").alias(x) for x in tracked + SCD_COLUMNS]

    def snapshot_cols() -> list[F.Column]:
        return [F.col(k) for k in keys] + [F.col(f"s.{x}").alias(x) for x in tracked + ["row_hash"]]

    kept = joined.filter(unchanged | still_removed).select(*current_cols())

    to_close = (
        joined.filter(changed | removed)
        .select(*current_cols())
        .withColumn("valid_to", eff)
        .withColumn("is_current", F.lit(False))
    )

    new_versions = _open_version(joined.filter(changed | new_key).select(*snapshot_cols()), eff, deleted=False)

    # A tombstone carries the last known attributes so downstream joins still resolve.
    tombstones = _open_version(
        joined.filter(removed).select(*current_cols()).drop("valid_from", "valid_to", "is_current", "is_deleted"),
        eff,
        deleted=True,
    )

    out_cols = business_cols + SCD_COLUMNS
    return (
        closed.select(*out_cols)
        .unionByName(kept.select(*out_cols))
        .unionByName(to_close.select(*out_cols))
        .unionByName(new_versions.select(*out_cols))
        .unionByName(tombstones.select(*out_cols))
    )
