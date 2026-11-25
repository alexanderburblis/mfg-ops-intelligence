"""End-to-end tests: generate data, run all three layers, check the numbers reconcile.

These run in CI on every push, so a change that breaks a rule, a join or a
KPI definition is caught before it reaches the lakehouse.
"""

from __future__ import annotations

import pytest

from mfgops.generate import generate
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.pipeline import run_bronze, run_gold, run_silver
from mfgops.quality import RULES


@pytest.fixture(scope="session")
def built(tmp_path_factory):
    base = tmp_path_factory.mktemp("lake")
    planted = generate(base / "raw", seed=7)
    spark = get_spark("mfgops-tests")
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=str(base / "lakehouse"), raw_path=str(base / "raw"))
    bronze = run_bronze(lh)
    silver = run_silver(lh, "test-run")
    gold = run_gold(lh)
    yield {"lh": lh, "planted": planted, "bronze": bronze, "silver": silver, "gold": gold}
    spark.stop()


def _failed(lh, rule_id):
    row = lh.read("gold", "dq_results").filter(f"rule_id = '{rule_id}'").collect()[0]
    return row["rows_failed"]


def test_every_rule_logs_a_result(built):
    logged = {r["rule_id"] for r in built["lh"].read("gold", "dq_results").collect()}
    assert logged == {r.rule_id for r in RULES}


@pytest.mark.parametrize(
    "rule_id, planted_key, factor",
    [
        ("SO-01", "so_unknown_customer", 1),
        ("SO-02", "so_negative_qty", 1),
        ("SO-03", "so_missing_requested_date", 1),
        ("PR-02", "prod_finish_before_start", 1),
        ("PR-03", "prod_implausible_yield", 1),
        ("PR-01", "prod_duplicate_id", 2),  # both copies of a duplicated order fail
        ("IN-01", "inv_negative_stock", 1),
        ("DL-01", "dlv_orphan", 1),
    ],
)
def test_planted_defects_are_caught(built, rule_id, planted_key, factor):
    assert _failed(built["lh"], rule_id) == built["planted"][planted_key] * factor


def test_bronze_keeps_d365_names_silver_is_canonical(built):
    lh = built["lh"]
    assert "SalesOrderNumber" in lh.read("bronze", "sales_orders").columns
    assert "so_id" in lh.read("silver", "sales_orders").columns


def test_bronze_equals_silver_plus_quarantine(built):
    for table, counts in built["silver"].items():
        assert counts["valid"] + counts["quarantined"] == built["bronze"][table], table


def test_silver_has_no_error_rows(built):
    so = built["lh"].read("silver", "sales_orders")
    assert so.filter("order_qty <= 0 OR requested_date IS NULL").count() == 0
    po = built["lh"].read("silver", "production_orders")
    assert po.groupBy("prod_order_id").count().filter("count > 1").count() == 0


def test_gold_sales_reconciles_to_silver(built):
    lh = built["lh"]
    silver_qty = lh.read("silver", "sales_orders").agg({"order_qty": "sum"}).collect()[0][0]
    gold_qty = lh.read("gold", "fact_sales_order_line").agg({"order_qty": "sum"}).collect()[0][0]
    assert silver_qty == gold_qty


def test_kpis_in_plausible_ranges(built):
    lh = built["lh"]
    s = lh.read("gold", "fact_sales_order_line").filter("is_shipped")
    otif = s.filter("is_otif").count() / s.count()
    assert 0.5 < otif < 0.95
    p = (
        lh.read("gold", "fact_production_order")
        .filter("is_completed")
        .agg({"scrap_qty": "sum", "output_qty": "sum"})
        .collect()[0]
    )
    scrap_rate = p["sum(scrap_qty)"] / p["sum(output_qty)"]
    assert 0.005 < scrap_rate < 0.10


def test_every_fact_key_has_a_dimension_row(built):
    lh = built["lh"]
    facts = lh.read("gold", "fact_sales_order_line")
    orphans = facts.join(lh.read("gold", "dim_customer"), "customer_sk", "left_anti").count()
    assert orphans == 0
    assert facts.filter("customer_sk = -1").count() == 0  # every valid order finds a customer version
    orphans = facts.join(lh.read("gold", "dim_material"), "material_id", "left_anti").count()
    assert orphans == 0


def test_quarantine_lists_failed_rules(built):
    q = built["lh"].read("quarantine", "sales_orders")
    assert q.count() > 0
    assert q.filter("failed_rules = '' OR failed_rules IS NULL").count() == 0


def test_new_rules_pass_on_clean_data(built):
    lh = built["lh"]
    for rule in ["SO-05", "SO-06", "PR-05", "MA-01", "MA-02", "PL-01", "TY-SAL", "TY-PRO", "TY-INV"]:
        assert _failed(lh, rule) == 0, rule
