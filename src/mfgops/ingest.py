"""Source contracts, schema validation and profiling, run before anything is loaded.

A source extract is only loaded if it matches its contract: the file exists,
it has exactly the expected columns, and it is not empty. A broken extract
fails the load with a clear message instead of silently producing nulls
further down the pipeline.
"""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from mfgops.lakehouse import Lakehouse

# Column contract per source extract (order does not matter, names and count do).
CONTRACTS: dict[str, list[str]] = {
    "plants": ["plant_id", "plant_name", "country", "currency"],
    "materials": [
        "material_id",
        "description",
        "material_type",
        "product_family",
        "base_uom",
        "standard_cost_chf",
        "safety_stock_qty",
        "lead_time_days",
    ],
    "bom": ["parent_material_id", "component_material_id", "qty_per"],
    "work_centers": ["work_center_id", "plant_id", "work_center_name", "capacity_hours_per_day"],
    "customers": ["customer_id", "customer_name", "region", "segment"],
    "suppliers": ["supplier_id", "supplier_name", "country"],
    "sales_orders": [
        "so_id",
        "so_line",
        "customer_id",
        "material_id",
        "plant_id",
        "order_date",
        "requested_date",
        "order_qty",
        "net_price_chf",
    ],
    "deliveries": ["delivery_id", "so_id", "so_line", "actual_ship_date", "shipped_qty"],
    "production_orders": [
        "prod_order_id",
        "plant_id",
        "material_id",
        "work_center_id",
        "planned_start",
        "planned_finish",
        "actual_start",
        "actual_finish",
        "planned_qty",
        "good_qty",
        "scrap_qty",
        "status",
    ],
    "inventory_snapshots": ["snapshot_date", "plant_id", "material_id", "on_hand_qty"],
    "purchase_orders": [
        "purchase_order_id",
        "supplier_id",
        "material_id",
        "plant_id",
        "order_date",
        "promised_date",
        "received_date",
        "order_qty",
    ],
}


class SourceValidationError(RuntimeError):
    """Raised when one or more source extracts break their contract."""

    def __init__(self, problems: list[str]):
        self.problems = problems
        super().__init__("Source validation failed:\n  - " + "\n  - ".join(problems))


def validate_sources(lh: Lakehouse) -> dict[str, int]:
    """Check every extract against its contract. Return row counts, or raise with every problem found."""
    problems, counts = [], {}
    for name, expected in CONTRACTS.items():
        try:
            df = lh.read_raw(name)
        except Exception as exc:  # missing file, unreadable file
            problems.append(f"{name}: cannot read source file ({type(exc).__name__})")
            continue
        missing = sorted(set(expected) - set(df.columns))
        extra = sorted(set(df.columns) - set(expected))
        if missing:
            problems.append(f"{name}: missing columns {missing}")
        if extra:
            problems.append(f"{name}: unexpected columns {extra}")
        n = df.count()
        if n == 0:
            problems.append(f"{name}: file is empty")
        counts[name] = n
    if problems:
        raise SourceValidationError(problems)
    return counts


PROFILE_SCHEMA = (
    "run_id string, profiled_at timestamp, table_name string, column_name string, "
    "row_count long, null_count long, null_pct double, distinct_count long, min_value string, max_value string"
)


def profile(lh: Lakehouse, df: DataFrame, table: str, run_id: str) -> DataFrame:
    """One row per column: nulls, distinct values, min and max (as text). Feeds the data dictionary."""
    cols = [c for c in df.columns if not c.startswith("_")]
    aggs = []
    for c in cols:
        aggs += [
            F.sum(F.col(c).isNull().cast("int")).alias(f"{c}__nulls"),
            F.approx_count_distinct(c).alias(f"{c}__distinct"),
            F.min(c).cast("string").alias(f"{c}__min"),
            F.max(c).cast("string").alias(f"{c}__max"),
        ]
    stats = df.agg(F.count(F.lit(1)).alias("__rows"), *aggs).first()
    n = stats["__rows"]
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = [
        (
            run_id,
            now,
            table,
            c,
            n,
            stats[f"{c}__nulls"],
            round(stats[f"{c}__nulls"] / n * 100, 2) if n else 0.0,
            stats[f"{c}__distinct"],
            stats[f"{c}__min"],
            stats[f"{c}__max"],
        )
        for c in cols
    ]
    return lh.spark.createDataFrame(rows, PROFILE_SCHEMA)
