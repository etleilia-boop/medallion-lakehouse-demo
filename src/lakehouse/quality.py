"""Data quality checks run after each pipeline step. Any failure stops the run."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    failed_rows: int

    @property
    def passed(self) -> bool:
        return self.failed_rows == 0

    def __str__(self) -> str:
        return f"[{'PASS' if self.passed else 'FAIL'}] {self.name} ({self.failed_rows} failing rows)"


class DataQualityError(RuntimeError):
    pass


def unique_key(df: DataFrame, keys: list[str], name: str) -> CheckResult:
    dupes = df.groupBy(*keys).count().filter("count > 1").count()
    return CheckResult(f"{name}: {', '.join(keys)} is unique", dupes)


def scd2_one_current_per_key(history: DataFrame, keys: list[str]) -> CheckResult:
    bad = history.filter("is_current").groupBy(*keys).count().filter("count != 1").count()
    keys_without_current = (
        history.groupBy(*keys).agg(F.max(F.col("is_current").cast("int")).alias("c")).filter("c = 0").count()
    )
    return CheckResult("scd2: exactly one current row per key", bad + keys_without_current)


def scd2_no_overlaps(history: DataFrame, keys: list[str]) -> CheckResult:
    """Versions of a key must be contiguous: each valid_to equals the next valid_from."""
    w = Window.partitionBy(*keys).orderBy("valid_from")
    gaps = (
        history.withColumn("_next_from", F.lead("valid_from").over(w))
        .filter(F.col("_next_from").isNotNull() & (F.col("valid_to") != F.col("_next_from")))
        .count()
    )
    inverted = history.filter(F.col("valid_from") >= F.col("valid_to")).count()
    return CheckResult("scd2: validity windows contiguous, no overlaps", gaps + inverted)


def not_null(df: DataFrame, cols: list[str], name: str) -> CheckResult:
    cond = None
    for c in cols:
        cond = F.col(c).isNull() if cond is None else cond | F.col(c).isNull()
    return CheckResult(f"{name}: {', '.join(cols)} not null", df.filter(cond).count())


def enforce(results: list[CheckResult]) -> None:
    for r in results:
        print(f"  {r}")
    failed = [r for r in results if not r.passed]
    if failed:
        raise DataQualityError("; ".join(r.name for r in failed))
