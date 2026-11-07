"""End-to-end run over the sample data shipped in data/landing."""

import shutil
from pathlib import Path

import pytest

from lakehouse.config import LakehouseConfig
from lakehouse.pipeline import run

SAMPLE_LANDING = Path(__file__).resolve().parents[1] / "data" / "landing"


@pytest.fixture
def cfg(tmp_path):
    shutil.copytree(SAMPLE_LANDING, tmp_path / "landing")
    return LakehouseConfig(root=tmp_path)


def read(spark, cfg, layer, name):
    return spark.read.parquet(cfg.table(layer, name))


def test_end_to_end_and_rerun_is_idempotent(spark, cfg):
    run(spark, cfg)
    history_1 = sorted(read(spark, cfg, "silver", "supplier_history").drop("row_hash").collect())

    run(spark, cfg)  # nothing new landed
    history_2 = sorted(read(spark, cfg, "silver", "supplier_history").drop("row_hash").collect())
    assert history_1 == history_2

    dim = {r.supplier_id for r in read(spark, cfg, "gold", "dim_supplier_current").collect()}
    assert dim == {"S001", "S002", "S004", "S005"}  # S003 dropped out of the March snapshot

    quarantined = {r.po_id for r in read(spark, cfg, "quarantine", "purchase_orders").collect()}
    assert quarantined == {"PO-1008", "PO-1011"}


def test_new_landing_file_is_picked_up_incrementally(spark, cfg):
    run(spark, cfg)
    (cfg.landing / "suppliers" / "suppliers_2026-04-01.csv").write_text(
        "supplier_id,supplier_name,country,category,payment_terms_days,is_active\n"
        "S001,Northwind Components,FR,Hardware,60,true\n"
        "S002,Blue Harbor Logistics,BE,Logistics,45,true\n"
        "S003,Atlas Office Supply,DE,Office,30,true\n"
        "S004,Cedar Consulting,GB,IT Services,60,true\n"
        "S005,Lumen Cloud,IE,Software,30,true\n"
    )
    run(spark, cfg)

    history = read(spark, cfg, "silver", "supplier_history")
    s001 = history.filter("supplier_id = 'S001' AND is_current").first()
    assert s001.payment_terms_days == 60
    s003 = history.filter("supplier_id = 'S003' AND is_current").first()
    assert s003.is_deleted is False  # reactivated
