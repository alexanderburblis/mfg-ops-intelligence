"""Incremental load: change extract, watermark, MERGE into silver, customer history (SCD type 2)."""

from __future__ import annotations

import pytest

from mfgops.generate import build_increment, generate, generate_increment
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.orchestrate import run_pipeline
from mfgops.pipeline import read_watermarks, run_silver


@pytest.fixture(scope="module")
def env(tmp_path_factory):
    base = tmp_path_factory.mktemp("inc")
    generate(base / "raw", seed=5)
    inc_info = generate_increment(base / "inc", seed=5)
    spark = get_spark("mfgops-inc-tests")
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=str(base / "lake"), raw_path=str(base / "raw"))
    full = run_pipeline(lh)
    before = {
        "sales": lh.read("silver", "sales_orders").count(),
        "history": lh.read("silver", "customers_history").count(),
        "started": lh.read("silver", "production_orders").filter("status = 'Started'").count(),
    }
    lh.raw_path = str(base / "inc")
    inc = run_pipeline(lh, mode="incremental")
    return {"lh": lh, "full": full, "inc": inc, "before": before, "info": inc_info}


def test_both_runs_succeed_and_reconcile(env):
    assert env["full"].status == "success"
    assert env["inc"].status == "success", env["inc"].message
    assert all(c["passed"] for c in env["inc"].outputs["reconciliation"])


def test_new_order_lines_are_inserted(env):
    lh = env["lh"]
    added = lh.read("silver", "sales_orders").count() - env["before"]["sales"]
    assert added == env["info"]["rows_sales_orders"] - env["info"]["inc_so_negative_qty"]


def test_rules_run_on_the_delta(env):
    q = env["lh"].read("quarantine", "sales_orders")
    assert q.count() == env["info"]["inc_so_negative_qty"]
    assert set(r["failed_rules"] for r in q.collect()) == {"SO-02"}


def test_changed_rows_are_updated_not_duplicated(env):
    lh = env["lh"]
    po = lh.read("silver", "production_orders")
    assert po.groupBy("prod_order_id").count().filter("count > 1").count() == 0
    changes, _ = build_increment(seed=5)
    ended = set(changes["production_orders"].query("status == 'Ended'").prod_order_id)
    got = {r["prod_order_id"] for r in po.filter("status = 'Ended'").select("prod_order_id").collect()}
    assert ended <= got


def test_customer_history_scd2(env):
    lh = env["lh"]
    hist = lh.read("silver", "customers_history")
    changes, _ = build_increment(seed=5)
    n_changed = len(changes["customers"])
    assert hist.count() == env["before"]["history"] + n_changed
    assert hist.filter("is_current").groupBy("customer_id").count().filter("count > 1").count() == 0
    closed = hist.filter("NOT is_current")
    assert closed.count() == n_changed - 3  # three customers are new, the rest moved segment
    assert closed.filter("valid_to = date '2026-11-14'").count() == closed.count()


def test_sales_use_the_customer_version_valid_on_the_order_date(env):
    lh = env["lh"]
    f = lh.read("gold", "fact_sales_order_line")
    d = lh.read("gold", "dim_customer")
    j = f.join(d, "customer_sk")
    assert f.filter("customer_sk = -1").count() == 0
    assert j.filter("order_date < valid_from OR order_date > valid_to").count() == 0


def test_watermarks_stored(env):
    marks = read_watermarks(env["lh"])
    assert marks["sales_orders"] == "2026-11-15T02:00:00"
    assert marks["plants"] == "2026-10-31T23:00:00"


def test_rerun_of_the_same_delivery_changes_nothing(env):
    lh = env["lh"]
    sales = lh.read("silver", "sales_orders").count()
    hist = lh.read("silver", "customers_history").count()
    # Silver again from the same bronze rows, as after a failed run where the watermark did not move.
    marks = read_watermarks(lh)
    marks["sales_orders"] = "2026-10-31T23:00:00"
    rows = [(t, w, "x") for t, w in marks.items()]
    lh.write(lh.spark.createDataFrame(rows, "table_name string, watermark string, run_id string"), "meta", "watermarks")
    run_silver(lh, "rerun", incremental=True)
    assert lh.read("silver", "sales_orders").count() == sales
    assert lh.read("silver", "customers_history").count() == hist


def test_gold_schema_module_matches_the_pipeline_output(env):
    from mfgops.gold_schema import GOLD_SCHEMA

    lh = env["lh"]
    for name, cols in GOLD_SCHEMA.items():
        layer, table = name.split("_", 1)
        actual = [(f.name, f.dataType.simpleString()) for f in lh.read(layer, table).schema.fields]
        assert actual == [tuple(c) for c in cols], name
