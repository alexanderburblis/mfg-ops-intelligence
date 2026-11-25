"""Medallion pipeline: bronze (raw as-is), silver (typed, checked, clean), gold (star schema).

Each layer is a plain function taking a ``Lakehouse``, so the same code runs
from the Fabric notebooks, from the local runner and from the tests.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from mfgops.lakehouse import Lakehouse
from mfgops.quality import apply_rules

SOURCE_TABLES = [
    "plants",
    "materials",
    "bom",
    "work_centers",
    "customers",
    "suppliers",
    "sales_orders",
    "deliveries",
    "production_orders",
    "inventory_snapshots",
    "purchase_orders",
]

# Column types applied in silver. Anything not listed stays a string.
SCHEMA: dict[str, dict[str, str]] = {
    "materials": {"standard_cost_chf": "decimal(18,2)", "safety_stock_qty": "int", "lead_time_days": "int"},
    "bom": {"qty_per": "int"},
    "work_centers": {"capacity_hours_per_day": "int"},
    "sales_orders": {
        "so_line": "int",
        "order_date": "date",
        "requested_date": "date",
        "order_qty": "int",
        "net_price_chf": "decimal(18,2)",
    },
    "deliveries": {"so_line": "int", "actual_ship_date": "date", "shipped_qty": "int"},
    "production_orders": {
        "planned_start": "date",
        "planned_finish": "date",
        "actual_start": "date",
        "actual_finish": "date",
        "planned_qty": "int",
        "good_qty": "int",
        "scrap_qty": "int",
    },
    "inventory_snapshots": {"snapshot_date": "date", "on_hand_qty": "int"},
    "purchase_orders": {"order_date": "date", "promised_date": "date", "received_date": "date", "order_qty": "int"},
}


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:6]


# --------------------------------------------------------------------------- bronze
def run_bronze(lh: Lakehouse) -> dict[str, int]:
    counts = {}
    for name in SOURCE_TABLES:
        df = (
            lh.read_raw(name)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(f"{name}.csv"))
        )
        lh.write(df, "bronze", name)
        counts[name] = df.count()
    return counts


# --------------------------------------------------------------------------- silver
def _typed(df: DataFrame, name: str) -> DataFrame:
    df = df.drop("_source_file")
    for col, typ in SCHEMA.get(name, {}).items():
        df = df.withColumn(col, F.col(col).cast(typ))
    return df


def _with_ref_columns(lh: Lakehouse, df: DataFrame, name: str) -> DataFrame:
    """Add the ``_ref_`` helper columns the integrity and uniqueness rules use."""
    if name == "sales_orders":
        known = (
            lh.read("bronze", "customers")
            .select("customer_id")
            .distinct()
            .withColumn("_ref_customer_known", F.lit(True))
        )
        return df.join(known, "customer_id", "left").fillna({"_ref_customer_known": False})
    if name == "deliveries":
        lines = (
            lh.read("bronze", "sales_orders")
            .select("so_id", F.col("so_line").cast("int").alias("so_line"))
            .distinct()
            .withColumn("_ref_so_line_known", F.lit(True))
        )
        return df.join(lines, ["so_id", "so_line"], "left").fillna({"_ref_so_line_known": False})
    if name == "production_orders":
        return df.withColumn("_ref_dup_count", F.count("*").over(Window.partitionBy("prod_order_id")))
    return df


def run_silver(lh: Lakehouse, run_id: str | None = None) -> dict[str, dict[str, int]]:
    run_id = run_id or new_run_id()
    summary = {}
    all_results = None
    for name in SOURCE_TABLES:
        df = _with_ref_columns(lh, _typed(lh.read("bronze", name), name), name)
        valid, quarantine, results = apply_rules(lh.spark, df, name, run_id)
        valid = valid.drop("_ingested_at").withColumn("_silver_run_id", F.lit(run_id))
        lh.write(valid, "silver", name)
        q = quarantine.count()
        if q:
            lh.write(quarantine, "quarantine", name)
        summary[name] = {"valid": valid.count(), "quarantined": q}
        all_results = results if all_results is None else all_results.unionByName(results)
    if all_results is not None:
        lh.append(all_results, "gold", "dq_results")
    return summary


# --------------------------------------------------------------------------- gold
def _dim_date(lh: Lakehouse) -> DataFrame:
    d = lh.spark.sql("SELECT explode(sequence(to_date('2025-01-01'), to_date('2026-12-31'), interval 1 day)) AS date")
    return (
        d.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("date"))
        .withColumn("quarter", F.concat(F.lit("Q"), F.quarter("date")))
        .withColumn("month_no", F.month("date"))
        .withColumn("month", F.date_format("date", "MMM"))
        .withColumn("year_month", F.date_format("date", "yyyy-MM"))
        .withColumn("iso_week", F.weekofyear("date"))
        .withColumn("is_weekday", F.dayofweek("date").between(2, 6))
    )


def _date_key(col: str) -> F.Column:
    return F.date_format(F.col(col), "yyyyMMdd").cast("int")


def _fact_sales(lh: Lakehouse) -> DataFrame:
    so = lh.read("silver", "sales_orders")
    dl = (
        lh.read("silver", "deliveries")
        .groupBy("so_id", "so_line")
        .agg(
            F.min("actual_ship_date").alias("first_ship_date"),
            F.max("actual_ship_date").alias("last_ship_date"),
            F.sum("shipped_qty").alias("shipped_qty"),
        )
    )
    f = so.join(dl, ["so_id", "so_line"], "left").fillna({"shipped_qty": 0})
    return (
        f.withColumn("order_value_chf", (F.col("order_qty") * F.col("net_price_chf")).cast("decimal(18,2)"))
        .withColumn("is_shipped", F.col("last_ship_date").isNotNull())
        .withColumn("is_on_time", F.col("is_shipped") & (F.col("last_ship_date") <= F.col("requested_date")))
        .withColumn("is_in_full", F.col("shipped_qty") >= F.col("order_qty"))
        .withColumn("is_otif", F.col("is_on_time") & F.col("is_in_full"))
        .withColumn("days_late", F.greatest(F.datediff("last_ship_date", "requested_date"), F.lit(0)))
        .withColumn("order_date_key", _date_key("order_date"))
        .withColumn("requested_date_key", _date_key("requested_date"))
        .withColumn("ship_date_key", _date_key("last_ship_date"))
        .drop("_silver_run_id")
    )


def _fact_production(lh: Lakehouse) -> DataFrame:
    po = lh.read("silver", "production_orders")
    cost = lh.read("silver", "materials").select("material_id", "standard_cost_chf")
    return (
        po.join(cost, "material_id", "left")
        .withColumn("is_completed", F.col("status") == "TECO")
        .withColumn("output_qty", F.col("good_qty") + F.col("scrap_qty"))
        .withColumn("cycle_days", F.datediff("actual_finish", "actual_start"))
        .withColumn("start_delay_days", F.datediff("actual_start", "planned_start"))
        .withColumn("is_on_schedule", F.col("is_completed") & (F.col("actual_finish") <= F.col("planned_finish")))
        .withColumn("good_value_chf", (F.col("good_qty") * F.col("standard_cost_chf")).cast("decimal(18,2)"))
        .withColumn("scrap_value_chf", (F.col("scrap_qty") * F.col("standard_cost_chf")).cast("decimal(18,2)"))
        .withColumn("planned_finish_key", _date_key("planned_finish"))
        .withColumn("actual_finish_key", _date_key("actual_finish"))
        .drop("_silver_run_id", "standard_cost_chf")
    )


def _avg_daily_demand(lh: Lakehouse) -> DataFrame:
    """Independent demand (sales) plus dependent demand (production orders exploded one BOM level)."""
    so = lh.read("silver", "sales_orders")
    days = so.agg(F.datediff(F.max("order_date"), F.min("order_date")).alias("d")).collect()[0]["d"] or 1
    independent = so.groupBy("plant_id", "material_id").agg(F.sum("order_qty").alias("qty"))
    bom = lh.read("silver", "bom")
    dependent = (
        lh.read("silver", "production_orders")
        .join(bom, F.col("material_id") == F.col("parent_material_id"))
        .groupBy("plant_id", F.col("component_material_id").alias("material_id"))
        .agg(F.sum(F.col("planned_qty") * F.col("qty_per")).alias("qty"))
    )
    return (
        independent.unionByName(dependent)
        .groupBy("plant_id", "material_id")
        .agg((F.sum("qty") / F.lit(days)).alias("avg_daily_demand"))
    )


def _fact_inventory(lh: Lakehouse) -> DataFrame:
    inv = lh.read("silver", "inventory_snapshots")
    mat = lh.read("silver", "materials").select("material_id", "standard_cost_chf", "safety_stock_qty")
    demand = _avg_daily_demand(lh)
    return (
        inv.join(mat, "material_id", "left")
        .join(demand, ["plant_id", "material_id"], "left")
        .withColumn("stock_value_chf", (F.col("on_hand_qty") * F.col("standard_cost_chf")).cast("decimal(18,2)"))
        .withColumn("is_below_safety_stock", F.col("on_hand_qty") < F.col("safety_stock_qty"))
        .withColumn(
            "days_of_coverage",
            F.when(F.col("avg_daily_demand") > 0, F.round(F.col("on_hand_qty") / F.col("avg_daily_demand"), 1)),
        )
        .withColumn("snapshot_date_key", _date_key("snapshot_date"))
        .drop("_silver_run_id", "standard_cost_chf", "safety_stock_qty")
    )


def _fact_purchasing(lh: Lakehouse) -> DataFrame:
    pu = lh.read("silver", "purchase_orders")
    return (
        pu.withColumn("is_received", F.col("received_date").isNotNull())
        .withColumn("is_on_time", F.col("is_received") & (F.col("received_date") <= F.col("promised_date")))
        .withColumn("days_late", F.greatest(F.datediff("received_date", "promised_date"), F.lit(0)))
        .withColumn("order_date_key", _date_key("order_date"))
        .withColumn("promised_date_key", _date_key("promised_date"))
        .drop("_silver_run_id")
    )


def run_gold(lh: Lakehouse) -> dict[str, int]:
    tables = {
        "dim_date": _dim_date(lh),
        "dim_plant": lh.read("silver", "plants").drop("_silver_run_id"),
        "dim_material": lh.read("silver", "materials").drop("_silver_run_id"),
        "dim_customer": lh.read("silver", "customers").drop("_silver_run_id"),
        "dim_supplier": lh.read("silver", "suppliers").drop("_silver_run_id"),
        "dim_work_center": lh.read("silver", "work_centers").drop("_silver_run_id"),
        "fact_sales_order_line": _fact_sales(lh),
        "fact_production_order": _fact_production(lh),
        "fact_inventory_snapshot": _fact_inventory(lh),
        "fact_purchase_order": _fact_purchasing(lh),
    }
    counts = {}
    for name, df in tables.items():
        lh.write(df, "gold", name)
        counts[name] = lh.read("gold", name).count()
    return counts
