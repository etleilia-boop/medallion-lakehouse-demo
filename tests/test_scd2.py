from datetime import datetime

from lakehouse.scd2 import apply_scd2

KEYS = ["id"]
TRACKED = ["name", "tier"]
D1, D2, D3, D4 = (datetime(2026, m, 1) for m in (1, 2, 3, 4))


def snap(spark, rows):
    return spark.createDataFrame(rows, "id string, name string, tier int")


def versions(history, key):
    rows = history.filter(f"id = '{key}'").orderBy("valid_from").collect()
    return [(r["name"], r["tier"], r["is_current"], r["is_deleted"]) for r in rows]


def test_initial_load_opens_one_current_version_per_key(spark):
    h = apply_scd2(None, snap(spark, [("A", "Alpha", 1), ("B", "Beta", 2)]), KEYS, TRACKED, D1)
    assert h.count() == 2
    assert h.filter("is_current AND NOT is_deleted").count() == 2
    assert str(h.first()["valid_to"]).startswith("9999-12-31")


def test_unchanged_snapshot_is_idempotent(spark):
    s = snap(spark, [("A", "Alpha", 1)])
    h1 = apply_scd2(None, s, KEYS, TRACKED, D1)
    h2 = apply_scd2(h1, s, KEYS, TRACKED, D2)
    assert sorted(h1.collect()) == sorted(h2.collect())


def test_change_closes_old_version_and_opens_new_one(spark):
    h = apply_scd2(None, snap(spark, [("A", "Alpha", 1)]), KEYS, TRACKED, D1)
    h = apply_scd2(h, snap(spark, [("A", "Alpha", 2)]), KEYS, TRACKED, D2)

    assert versions(h, "A") == [("Alpha", 1, False, False), ("Alpha", 2, True, False)]
    old, new = (
        h.orderBy("valid_from")
        .selectExpr(
            "date_format(valid_to, 'yyyy-MM-dd') AS valid_to", "date_format(valid_from, 'yyyy-MM-dd') AS valid_from"
        )
        .collect()
    )
    assert old["valid_to"] == new["valid_from"] == "2026-02-01"


def test_missing_key_becomes_tombstone_then_reactivates(spark):
    h = apply_scd2(None, snap(spark, [("A", "Alpha", 1), ("B", "Beta", 1)]), KEYS, TRACKED, D1)
    h = apply_scd2(h, snap(spark, [("A", "Alpha", 1)]), KEYS, TRACKED, D2)  # B disappears
    h = apply_scd2(h, snap(spark, [("A", "Alpha", 1)]), KEYS, TRACKED, D3)  # still gone: no new row
    assert versions(h, "B") == [("Beta", 1, False, False), ("Beta", 1, True, True)]

    h = apply_scd2(h, snap(spark, [("A", "Alpha", 1), ("B", "Beta", 1)]), KEYS, TRACKED, D4)  # back
    assert versions(h, "B")[-1] == ("Beta", 1, True, False)
    assert len(versions(h, "B")) == 3


def test_null_and_value_are_different(spark):
    h = apply_scd2(None, snap(spark, [("A", None, 1)]), KEYS, TRACKED, D1)
    h = apply_scd2(h, snap(spark, [("A", "", 1)]), KEYS, TRACKED, D2)
    assert len(versions(h, "A")) == 2
