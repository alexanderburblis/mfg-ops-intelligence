"""Orchestration tests: gate, source validation, row-count monitoring."""

from __future__ import annotations

import shutil

import pandas as pd
import pytest

from mfgops.generate import generate
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.orchestrate import RowCountError, monitor_row_counts, run_pipeline


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    base = tmp_path_factory.mktemp("orch")
    generate(base / "raw", seed=11)
    spark = get_spark("mfgops-orch-tests")
    spark.sparkContext.setLogLevel("ERROR")
    yield base, Lakehouse(spark, root=str(base / "lake"), raw_path=str(base / "raw"))


def test_clean_run_succeeds_and_reconciles(env):
    _, lh = env
    res = run_pipeline(lh)
    assert res.status == "success"
    assert all(c["passed"] for c in res.outputs["reconciliation"])
    stages = [s["stage"] for s in res.stages]
    assert stages[0] == "validate_sources" and stages[-1] == "reconcile"


def test_gate_blocks_gold_on_bad_extract(env):
    base, lh = env
    gold_before = lh.read("gold", "fact_sales_order_line").count()
    path = base / "raw" / "SalesOrderLinesV2.csv"
    shutil.copy(path, base / "so.bak")
    so = pd.read_csv(path, dtype=str)
    so.loc[so.sample(frac=0.05, random_state=1).index, "ShippingSiteId"] = "DE01"
    so.to_csv(path, index=False)
    res = run_pipeline(lh)
    shutil.move(base / "so.bak", path)
    assert res.status == "failed" and res.failed_stage == "quality_gate"
    assert lh.read("gold", "fact_sales_order_line").count() == gold_before


def test_missing_column_fails_before_loading(env):
    base, lh = env
    path = base / "raw" / "SalesPackingSlipLines.csv"
    shutil.copy(path, base / "dl.bak")
    pd.read_csv(path, dtype=str).drop(columns=["DeliveredQuantity"]).to_csv(path, index=False)
    res = run_pipeline(lh)
    shutil.move(base / "dl.bak", path)
    assert res.status == "failed" and res.failed_stage == "validate_sources"
    assert "DeliveredQuantity" in res.message


def test_row_count_monitor():
    assert monitor_row_counts({"a": 100}, {"a": 110}) == []
    assert monitor_row_counts({"a": 100}, {"a": 140}) == ["a: 100 → 140 (+40%)"]
    with pytest.raises(RowCountError):
        monitor_row_counts({"a": 100}, {"a": 40})


def test_reconciliation_leaves_no_views_that_shadow_tables(env):
    # In Fabric, a temp view named like a lakehouse table hides the table from later reads and MERGE.
    _, lh = env
    from mfgops.reconcile import reconcile

    reconcile(lh)
    names = {t.name for t in lh.spark.catalog.listTables() if t.isTemporary}
    assert not any(n.startswith(("silver_", "gold_", "bronze_")) for n in names), names
