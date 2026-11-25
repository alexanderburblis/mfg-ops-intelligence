"""Medallion pipeline: bronze (raw as-is), silver (typed, checked, clean), gold (star schema).

Each layer is a plain function taking a ``Lakehouse``, so the same code runs
from the Fabric notebooks, from the local runner and from the tests.

Two load modes:

* ``full``: every extract is loaded, bronze and silver are replaced.
* ``incremental``: only the change extract is loaded. Bronze keeps every
  delivery (append). Silver takes the rows changed since the last successful
  run (``ModifiedDateTime`` above the stored watermark), checks them with the
  same rules and upserts them by business key (Delta ``MERGE`` in Fabric).
  Re-running the same delivery gives the same result, so a failed run can
  simply be repeated. Customers keep their history (slowly changing
  dimension, type 2) so sales are reported with the segment a customer had
  when the order was placed.

Gold is small enough to be rebuilt from silver on every run.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from mfgops.lakehouse import Lakehouse
from mfgops.quality import apply_rules
from mfgops.source_map import ENTITIES, SOURCE_TABLES, WATERMARK_COLUMN, to_canonical

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
def run_bronze(lh: Lakehouse, run_id: str = "manual", incremental: bool = False) -> dict[str, int]:
    """Land every extract unchanged (D365 column names), stamped with load time and batch id.

    Full load replaces bronze. Incremental load appends the delivery, so bronze
    is the complete history of what the source sent.
    """
    counts = {}
    for name, e in ENTITIES.items():
        try:
            df = lh.read_raw(e.file)
            df.columns  # noqa: B018  (forces the read, a missing incremental extract means no changes)
        except Exception:
            if incremental:
                counts[name] = 0
                continue
            raise
        df = (
            df.withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.lit(f"{e.file}.csv"))
            .withColumn("_batch_id", F.lit(run_id))
        )
        n = df.count()
        if incremental:
            if n:
                lh.append(df, "bronze", name)
        else:
            lh.write(df, "bronze", name)
        counts[name] = n
    return counts


def bronze_canonical(lh: Lakehouse, name: str) -> DataFrame:
    """Bronze table with canonical column names (for lookups)."""
    return to_canonical(lh.read("bronze", name), name)


# --------------------------------------------------------------------------- silver
def _typed(df: DataFrame, name: str) -> DataFrame:
    """Cast columns to their silver types and count values that fail to convert."""
    df = (
        df.drop("_source_file")
        .withColumn("_modified_at", F.to_timestamp(F.col(WATERMARK_COLUMN)))
        .drop(WATERMARK_COLUMN)
    )
    casts = SCHEMA.get(name, {})
    if not casts:
        return df
    bad = F.lit(0)
    for col, typ in casts.items():
        bad = bad + (F.col(col).isNotNull() & F.col(col).cast(typ).isNull()).cast("int")
    df = df.withColumn("_ref_type_errors", bad)
    for col, typ in casts.items():
        df = df.withColumn(col, F.col(col).cast(typ))
    return df


def _with_ref_columns(lh: Lakehouse, df: DataFrame, name: str) -> DataFrame:
    """Add the ``_ref_`` helper columns the integrity and uniqueness rules use."""
    if name == "sales_orders":
        known = (
            bronze_canonical(lh, "customers")
            .select("customer_id")
            .distinct()
            .withColumn("_ref_customer_known", F.lit(True))
        )
        plants = (
            bronze_canonical(lh, "plants").select("plant_id").distinct().withColumn("_ref_plant_known", F.lit(True))
        )
        return (
            df.join(known, "customer_id", "left")
            .join(plants, "plant_id", "left")
            .fillna({"_ref_customer_known": False, "_ref_plant_known": False})
        )
    if name == "deliveries":
        lines = (
            bronze_canonical(lh, "sales_orders")
            .select("so_id", F.col("so_line").cast("int").alias("so_line"))
            .distinct()
            .withColumn("_ref_so_line_known", F.lit(True))
        )
        return df.join(lines, ["so_id", "so_line"], "left").fillna({"_ref_so_line_known": False})
    if name == "production_orders":
        return df.withColumn("_ref_dup_count", F.count("*").over(Window.partitionBy("prod_order_id")))
    return df


def read_watermarks(lh: Lakehouse) -> dict[str, str]:
    if not lh.exists("meta", "watermarks"):
        return {}
    return {r["table_name"]: r["watermark"] for r in lh.read("meta", "watermarks").collect()}


def write_watermarks(lh: Lakehouse, marks: dict[str, str], run_id: str) -> None:
    """Store the high-water mark per table. Called only after a fully successful run."""
    old = read_watermarks(lh)
    old.update({k: v for k, v in marks.items() if v})
    rows = [(t, w, run_id) for t, w in sorted(old.items())]
    lh.write(lh.spark.createDataFrame(rows, "table_name string, watermark string, run_id string"), "meta", "watermarks")


def _changed_rows(lh: Lakehouse, name: str, watermark: str | None) -> DataFrame:
    """Bronze rows newer than the watermark. If the same delivery was landed twice
    (a rerun), only the latest landing of it is used."""
    df = lh.read("bronze", name)
    if watermark:
        df = df.filter(F.col(WATERMARK_COLUMN) > F.lit(watermark))
    latest = Window.partitionBy(WATERMARK_COLUMN)
    return (
        df.withColumn("_latest_batch", F.max("_batch_id").over(latest))
        .filter(F.col("_batch_id") == F.col("_latest_batch"))
        .drop("_latest_batch")
    )


def run_silver(lh: Lakehouse, run_id: str | None = None, incremental: bool = False) -> dict[str, dict]:
    run_id = run_id or new_run_id()
    marks = read_watermarks(lh) if incremental else {}
    summary = {}
    all_results = None
    for name in SOURCE_TABLES:
        e = ENTITIES[name]
        if incremental and not lh.exists("bronze", name):
            continue
        raw = _changed_rows(lh, name, marks.get(name)) if incremental else lh.read("bronze", name)
        new_mark = raw.agg(F.max(WATERMARK_COLUMN)).first()[0]
        if incremental and new_mark is None:
            summary[name] = {"valid": 0, "quarantined": 0, "error_rate": 0.0, "updated": 0, "inserted": 0}
            continue
        df = _with_ref_columns(lh, _typed(to_canonical(raw, name), name), name)
        valid, quarantine, results = apply_rules(lh.spark, df, name, run_id)
        # If two deliveries changed the same row, the newer version wins.
        newest = Window.partitionBy(*e.keys).orderBy(F.col("_modified_at").desc())
        valid = (
            valid.drop("_ingested_at", "_batch_id")
            .withColumn("_rn", F.row_number().over(newest))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("_silver_run_id", F.lit(run_id))
        )
        if incremental:
            merged = lh.merge(valid, "silver", name, e.keys)
        else:
            lh.write(valid, "silver", name)
            merged = {"updated": 0, "inserted": lh.read("silver", name).count()}
        if name == "customers":
            update_customer_history(lh, valid, full=not incremental)
        q = quarantine.count()
        lh.write(quarantine.drop("_batch_id"), "quarantine", name)  # latest run only
        checked = q + valid.count()
        summary[name] = {
            "valid": checked - q,
            "quarantined": q,
            "error_rate": round(q / max(checked, 1), 4),
            **merged,
            "watermark": new_mark,
        }
        all_results = results if all_results is None else all_results.unionByName(results)
    if all_results is not None:
        lh.append(all_results, "gold", "dq_results")
    return summary


HISTORY_ATTRS = ["customer_name", "region", "segment"]
OPEN_END = "9999-12-31"


def update_customer_history(lh: Lakehouse, changed: DataFrame, full: bool) -> None:
    """Slowly changing dimension type 2 for customers.

    A change in name, region or segment closes the current version (valid_to =
    day before the change) and opens a new one. New customers get a first
    version open from 1900-01-01, as does every customer in a full load, so
    every order finds the version valid on its order date.
    """
    cols = ["customer_id", *HISTORY_ATTRS]
    if full or not lh.exists("silver", "customers_history"):
        hist = (
            lh.read("silver", "customers")
            .select(*cols)
            .withColumn("valid_from", F.to_date(F.lit("1900-01-01")))
            .withColumn("valid_to", F.to_date(F.lit(OPEN_END)))
            .withColumn("is_current", F.lit(True))
            .withColumn("customer_sk", F.row_number().over(Window.orderBy("customer_id")).cast("long"))
        )
        lh.write(hist, "silver", "customers_history")
        return
    hist = lh.read("silver", "customers_history").cache()
    current = hist.filter("is_current")
    incoming = changed.select(*cols, F.to_date("_modified_at").alias("effective"))
    cmp = incoming.join(
        current.select("customer_id", *[F.col(c).alias(f"cur_{c}") for c in HISTORY_ATTRS]), "customer_id", "left"
    )
    differs = F.lit(False)
    for c in HISTORY_ATTRS:
        differs = differs | ~F.col(c).eqNullSafe(F.col(f"cur_{c}"))
    new_versions = cmp.filter(F.col("cur_customer_name").isNull() | differs).cache()
    if new_versions.count() == 0:
        return
    closing = new_versions.filter(F.col("cur_customer_name").isNotNull()).select("customer_id", "effective")
    closed = (
        hist.join(closing, "customer_id", "left")
        .withColumn(
            "valid_to",
            F.when(F.col("is_current") & F.col("effective").isNotNull(), F.date_sub("effective", 1)).otherwise(
                F.col("valid_to")
            ),
        )
        .withColumn("is_current", F.col("is_current") & F.col("effective").isNull())
        .drop("effective")
    )
    max_sk = hist.agg(F.max("customer_sk")).first()[0] or 0
    added = new_versions.select(
        *cols,
        # A brand-new customer's first version is open from the start, so its first orders find it.
        F.when(F.col("cur_customer_name").isNull(), F.to_date(F.lit("1900-01-01")))
        .otherwise(F.col("effective"))
        .alias("valid_from"),
        F.to_date(F.lit(OPEN_END)).alias("valid_to"),
        F.lit(True).alias("is_current"),
        (F.row_number().over(Window.orderBy("customer_id")) + F.lit(max_sk)).cast("long").alias("customer_sk"),
    )
    out = closed.unionByName(added).localCheckpoint()  # cut the lineage before overwriting the table it was read from
    lh.write(out, "silver", "customers_history")


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
    hist = lh.read("silver", "customers_history").select("customer_id", "customer_sk", "valid_from", "valid_to")
    f = (
        f.join(
            hist,
            (f.customer_id == hist.customer_id) & f.order_date.between(hist.valid_from, hist.valid_to),
            "left",
        )
        .drop(hist.customer_id)
        .drop("valid_from", "valid_to")
        .fillna({"customer_sk": -1})
    )
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
        .drop("_silver_run_id", "_modified_at")
    )


def _fact_production(lh: Lakehouse) -> DataFrame:
    po = lh.read("silver", "production_orders")
    cost = lh.read("silver", "materials").select("material_id", "standard_cost_chf")
    return (
        po.join(cost, "material_id", "left")
        .withColumn("is_completed", F.col("status") == "Ended")
        .withColumn("output_qty", F.col("good_qty") + F.col("scrap_qty"))
        .withColumn("cycle_days", F.datediff("actual_finish", "actual_start"))
        .withColumn("start_delay_days", F.datediff("actual_start", "planned_start"))
        .withColumn("is_on_schedule", F.col("is_completed") & (F.col("actual_finish") <= F.col("planned_finish")))
        .withColumn("good_value_chf", (F.col("good_qty") * F.col("standard_cost_chf")).cast("decimal(18,2)"))
        .withColumn("scrap_value_chf", (F.col("scrap_qty") * F.col("standard_cost_chf")).cast("decimal(18,2)"))
        .withColumn("planned_finish_key", _date_key("planned_finish"))
        .withColumn("actual_finish_key", _date_key("actual_finish"))
        .drop("_silver_run_id", "_modified_at", "standard_cost_chf")
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
        .drop("_silver_run_id", "_modified_at", "standard_cost_chf", "safety_stock_qty")
    )


def _fact_purchasing(lh: Lakehouse) -> DataFrame:
    pu = lh.read("silver", "purchase_orders")
    return (
        pu.withColumn("is_received", F.col("received_date").isNotNull())
        .withColumn("is_on_time", F.col("is_received") & (F.col("received_date") <= F.col("promised_date")))
        .withColumn("days_late", F.greatest(F.datediff("received_date", "promised_date"), F.lit(0)))
        .withColumn("order_date_key", _date_key("order_date"))
        .withColumn("promised_date_key", _date_key("promised_date"))
        .drop("_silver_run_id", "_modified_at")
    )


# Row-level security: which plants each user may see (fictional users of the fictional company).
# In production this list comes from an HR / identity source, not from code.
PLANT_ACCESS = [
    ("plant.manager.ch01@nordhalde.example", "CH01"),
    ("plant.manager.my01@nordhalde.example", "MY01"),
    ("plant.manager.ro01@nordhalde.example", "RO01"),
    ("ops.director@nordhalde.example", "CH01"),
    ("ops.director@nordhalde.example", "MY01"),
    ("ops.director@nordhalde.example", "RO01"),
]


def _security_plant_access(lh: Lakehouse) -> DataFrame:
    return lh.spark.createDataFrame(PLANT_ACCESS, "user_principal_name string, plant_id string")


def _dim_customer(lh: Lakehouse) -> DataFrame:
    """One row per customer version, plus an 'Unknown' member (key -1)."""
    hist = lh.read("silver", "customers_history")
    unknown = lh.spark.createDataFrame(
        [(-1, "UNKNOWN", "Unknown customer", "Unknown", "Unknown", "1900-01-01", OPEN_END, True)],
        "customer_sk long, customer_id string, customer_name string, region string, segment string, "
        "valid_from string, valid_to string, is_current boolean",
    ).select(
        "customer_sk",
        "customer_id",
        "customer_name",
        "region",
        "segment",
        F.to_date("valid_from").alias("valid_from"),
        F.to_date("valid_to").alias("valid_to"),
        "is_current",
    )
    return hist.select(*unknown.columns).unionByName(unknown)


def run_gold(lh: Lakehouse) -> dict[str, int]:
    tables = {
        "dim_date": _dim_date(lh),
        "dim_plant": lh.read("silver", "plants").drop("_silver_run_id", "_modified_at"),
        "dim_material": lh.read("silver", "materials").drop("_silver_run_id", "_modified_at"),
        "dim_customer": _dim_customer(lh),
        "dim_supplier": lh.read("silver", "suppliers").drop("_silver_run_id", "_modified_at"),
        "dim_work_center": lh.read("silver", "work_centers").drop("_silver_run_id", "_modified_at"),
        "fact_sales_order_line": _fact_sales(lh),
        "fact_production_order": _fact_production(lh),
        "fact_inventory_snapshot": _fact_inventory(lh),
        "fact_purchase_order": _fact_purchasing(lh),
        "security_plant_access": _security_plant_access(lh),
    }
    counts = {}
    for name, df in tables.items():
        lh.write(df, "gold", name)
        counts[name] = lh.read("gold", name).count()
    return counts
