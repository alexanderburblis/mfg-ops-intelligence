"""Data-quality rules, applied between bronze and silver.

Each rule names the rows that FAIL it. Rules with severity ``error`` send the
failing rows to quarantine; rules with severity ``warning`` only get logged.
Integrity and uniqueness checks rely on helper columns prefixed ``_ref_``
that the silver step adds before the rules run (lookups and counts). They are
dropped again from the output.
Every run appends one result row per rule to ``dq_results``, which feeds the
data-quality page of the report.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


@dataclass(frozen=True)
class Rule:
    rule_id: str
    table: str
    dimension: str  # completeness, validity, consistency, uniqueness, integrity
    severity: str  # error or warning
    description: str
    failing: str  # Spark SQL condition that is TRUE for a failing row


RULES: list[Rule] = [
    Rule(
        "SO-01", "sales_orders", "integrity", "error", "Customer exists in customer master", "NOT _ref_customer_known"
    ),
    Rule(
        "SO-02",
        "sales_orders",
        "validity",
        "error",
        "Order quantity is positive",
        "order_qty <= 0 OR order_qty IS NULL",
    ),
    Rule(
        "SO-03", "sales_orders", "completeness", "error", "Requested delivery date is filled", "requested_date IS NULL"
    ),
    Rule(
        "SO-04",
        "sales_orders",
        "validity",
        "warning",
        "Requested date is not before order date",
        "requested_date < order_date",
    ),
    Rule("PR-01", "production_orders", "uniqueness", "error", "Production order ID is unique", "_ref_dup_count > 1"),
    Rule(
        "PR-02",
        "production_orders",
        "consistency",
        "error",
        "Actual finish is not before actual start",
        "actual_finish < actual_start",
    ),
    Rule(
        "PR-03",
        "production_orders",
        "validity",
        "error",
        "Good plus scrap quantity is plausible (max 150% of plan)",
        "good_qty + scrap_qty > planned_qty * 1.5",
    ),
    Rule(
        "PR-04",
        "production_orders",
        "integrity",
        "warning",
        "Work centre belongs to the same plant",
        "substring(work_center_id, 1, 4) <> plant_id",
    ),
    Rule("IN-01", "inventory_snapshots", "validity", "error", "Stock on hand is not negative", "on_hand_qty < 0"),
    Rule(
        "DL-01",
        "deliveries",
        "integrity",
        "error",
        "Delivery refers to an existing sales order line",
        "NOT _ref_so_line_known",
    ),
    Rule(
        "PU-01",
        "purchase_orders",
        "consistency",
        "warning",
        "Goods receipt is not before the order date",
        "received_date < order_date",
    ),
]


RESULT_SCHEMA = (
    "run_id string, run_ts timestamp, rule_id string, table_name string, dimension string, "
    "severity string, description string, rows_checked long, rows_failed long"
)


def rules_for(table: str) -> list[Rule]:
    return [r for r in RULES if r.table == table]


def apply_rules(spark: SparkSession, df: DataFrame, table: str, run_id: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split ``df`` into (valid rows, quarantined rows, rule results)."""
    rules = rules_for(table)
    if not rules:
        empty = spark.createDataFrame([], RESULT_SCHEMA)
        return df, df.limit(0), empty
    flagged = df
    for r in rules:
        flagged = flagged.withColumn(
            f"_fail_{r.rule_id}", F.coalesce(F.expr(f"CASE WHEN {r.failing} THEN true ELSE false END"), F.lit(False))
        )
    total = flagged.count()
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)

    agg = flagged.agg(*[F.sum(F.col(f"_fail_{r.rule_id}").cast("int")).alias(r.rule_id) for r in rules]).collect()
    counts = agg[0].asDict() if rules else {}
    results = spark.createDataFrame(
        [
            (
                run_id,
                run_ts,
                r.rule_id,
                table,
                r.dimension,
                r.severity,
                r.description,
                total,
                int(counts.get(r.rule_id) or 0),
            )
            for r in rules
        ],
        RESULT_SCHEMA,
    )

    error_cols = [f"_fail_{r.rule_id}" for r in rules if r.severity == "error"]
    any_error = F.lit(False)
    for c in error_cols:
        any_error = any_error | F.col(c)
    flagged = flagged.withColumn("_has_error", any_error)

    failed_ids = (
        F.array_remove(
            F.array(*[F.when(F.col(f"_fail_{r.rule_id}"), F.lit(r.rule_id)) for r in rules if r.severity == "error"]),
            None,
        )
        if error_cols
        else F.array()
    )
    drop = [f"_fail_{r.rule_id}" for r in rules] + ["_has_error"] + [c for c in df.columns if c.startswith("_ref_")]
    valid = flagged.filter(~F.col("_has_error")).drop(*drop)
    quarantine = (
        flagged.filter(F.col("_has_error"))
        .withColumn("failed_rules", F.concat_ws(",", failed_ids))
        .withColumn("run_id", F.lit(run_id))
        .drop(*drop)
    )
    return valid, quarantine, results
