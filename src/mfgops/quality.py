"""Data-quality rules, applied between bronze and silver.

Each rule names the rows that FAIL it. Rules with severity ``error`` send the
failing rows to quarantine. Rules with severity ``warning`` only get logged.

Integrity, uniqueness and type checks rely on helper columns prefixed
``_ref_`` that the silver step adds before the rules run (lookups, counts,
cast failures). They are dropped again from the output.

Every rule has an owner and a corrective action, so a failing rule turns into
a task for a named team instead of a number on a dashboard. Every run appends
one result row per rule to ``dq_results``.
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
    dimension: str  # completeness, validity, consistency, uniqueness, integrity, type
    severity: str  # error or warning
    description: str
    failing: str  # Spark SQL condition that is TRUE for a failing row
    owner: str = "Data owner"
    action: str = "Correct in the source system and rerun"


# Tables whose columns are cast in silver get an automatic type rule (see TYPED_TABLES).
TYPED_TABLES = [
    "materials",
    "bom",
    "work_centers",
    "sales_orders",
    "deliveries",
    "production_orders",
    "inventory_snapshots",
    "purchase_orders",
]

RULES: list[Rule] = [
    # ---------------------------------------------------------------- sales orders
    Rule(
        "SO-01",
        "sales_orders",
        "integrity",
        "error",
        "Customer exists in customer master",
        "NOT _ref_customer_known",
        "Customer service",
        "Create or correct the customer in the ERP master data",
    ),
    Rule(
        "SO-02",
        "sales_orders",
        "validity",
        "error",
        "Order quantity is positive",
        "order_qty <= 0 OR order_qty IS NULL",
        "Customer service",
        "Correct the order line quantity",
    ),
    Rule(
        "SO-03",
        "sales_orders",
        "completeness",
        "error",
        "Requested delivery date is filled",
        "requested_date IS NULL",
        "Customer service",
        "Enter the customer's requested date on the order",
    ),
    Rule(
        "SO-04",
        "sales_orders",
        "validity",
        "warning",
        "Requested date is not before order date",
        "requested_date < order_date",
        "Customer service",
        "Check the order dates with the customer",
    ),
    Rule(
        "SO-05",
        "sales_orders",
        "integrity",
        "error",
        "Plant code exists in the plant master",
        "NOT _ref_plant_known",
        "ERP / IT",
        "Fix the plant mapping in the ERP extract",
    ),
    Rule(
        "SO-06",
        "sales_orders",
        "validity",
        "error",
        "Net price is positive",
        "net_price_chf <= 0 OR net_price_chf IS NULL",
        "Pricing",
        "Correct the price condition on the order",
    ),
    # ---------------------------------------------------------------- production
    Rule(
        "PR-01",
        "production_orders",
        "uniqueness",
        "error",
        "Production order ID is unique",
        "_ref_dup_count > 1",
        "ERP / IT",
        "Remove the duplicate from the extract and check the interface",
    ),
    Rule(
        "PR-02",
        "production_orders",
        "consistency",
        "error",
        "Actual finish is not before actual start",
        "actual_finish < actual_start",
        "Production planning",
        "Correct the confirmation dates",
    ),
    Rule(
        "PR-03",
        "production_orders",
        "validity",
        "error",
        "Good plus scrap quantity is plausible (max 150% of plan)",
        "good_qty + scrap_qty > planned_qty * 1.5",
        "Production",
        "Correct the yield or scrap confirmation",
    ),
    Rule(
        "PR-04",
        "production_orders",
        "integrity",
        "warning",
        "Work centre belongs to the same plant",
        "substring(work_center_id, 1, 4) <> plant_id",
        "Production planning",
        "Check the routing",
    ),
    Rule(
        "PR-05",
        "production_orders",
        "validity",
        "error",
        "Status is a known value (REL, TECO)",
        "status NOT IN ('REL', 'TECO') OR status IS NULL",
        "ERP / IT",
        "Map the new status in the extract",
    ),
    # ---------------------------------------------------------------- inventory
    Rule(
        "IN-01",
        "inventory_snapshots",
        "validity",
        "error",
        "Stock on hand is not negative",
        "on_hand_qty < 0",
        "Warehouse",
        "Post the missing goods receipt or correct the stock",
    ),
    # ---------------------------------------------------------------- deliveries
    Rule(
        "DL-01",
        "deliveries",
        "integrity",
        "error",
        "Delivery refers to an existing sales order line",
        "NOT _ref_so_line_known",
        "Logistics",
        "Link the delivery to its order in the ERP",
    ),
    # ---------------------------------------------------------------- purchasing
    Rule(
        "PU-01",
        "purchase_orders",
        "consistency",
        "warning",
        "Goods receipt is not before the order date",
        "received_date < order_date",
        "Procurement",
        "Check the goods receipt posting date",
    ),
    # ---------------------------------------------------------------- master data
    Rule(
        "MA-01",
        "materials",
        "validity",
        "error",
        "Material type is FERT, HALB or ROH",
        "material_type NOT IN ('FERT', 'HALB', 'ROH') OR material_type IS NULL",
        "Master data",
        "Correct the material type",
    ),
    Rule(
        "MA-02",
        "materials",
        "consistency",
        "warning",
        "Base unit of measure is PC (one unit across the model)",
        "base_uom <> 'PC' OR base_uom IS NULL",
        "Master data",
        "Add a unit conversion or correct the unit",
    ),
    Rule(
        "PL-01",
        "plants",
        "validity",
        "warning",
        "Plant currency is a supported currency",
        "currency NOT IN ('CHF', 'EUR', 'MYR', 'RON', 'USD')",
        "Finance",
        "Add the currency to FX translation",
    ),
] + [
    Rule(
        f"TY-{t[:3].upper()}",
        t,
        "type",
        "error",
        "Every typed column converts to its data type",
        "_ref_type_errors > 0",
        "ERP / IT",
        "Fix the field format in the extract",
    )
    for t in TYPED_TABLES
]


RESULT_SCHEMA = (
    "run_id string, run_ts timestamp, rule_id string, table_name string, dimension string, "
    "severity string, description string, owner string, corrective_action string, "
    "rows_checked long, rows_failed long"
)


def rules_for(table: str) -> list[Rule]:
    return [r for r in RULES if r.table == table]


def apply_rules(spark: SparkSession, df: DataFrame, table: str, run_id: str) -> tuple[DataFrame, DataFrame, DataFrame]:
    """Split ``df`` into (valid rows, quarantined rows, rule results)."""
    rules = rules_for(table)
    ref_cols = [c for c in df.columns if c.startswith("_ref_")]
    if not rules:
        return df.drop(*ref_cols), df.limit(0), spark.createDataFrame([], RESULT_SCHEMA)

    flagged = df
    for r in rules:
        flagged = flagged.withColumn(
            f"_fail_{r.rule_id}", F.coalesce(F.expr(f"CASE WHEN {r.failing} THEN true ELSE false END"), F.lit(False))
        )
    flagged = flagged.cache()
    total = flagged.count()
    run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    counts = flagged.agg(*[F.sum(F.col(f"_fail_{r.rule_id}").cast("int")).alias(r.rule_id) for r in rules]).first()
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
                r.owner,
                r.action,
                total,
                int(counts[r.rule_id] or 0),
            )
            for r in rules
        ],
        RESULT_SCHEMA,
    )

    error_rules = [r for r in rules if r.severity == "error"]
    any_error = F.lit(False)
    for r in error_rules:
        any_error = any_error | F.col(f"_fail_{r.rule_id}")
    # concat_ws skips nulls, so only the IDs of the rules a row failed end up in the list
    failed_ids = [F.when(F.col(f"_fail_{r.rule_id}"), F.lit(r.rule_id)) for r in error_rules]

    drop = [f"_fail_{r.rule_id}" for r in rules] + ["_has_error"] + ref_cols
    flagged = flagged.withColumn("_has_error", any_error)
    valid = flagged.filter(~F.col("_has_error")).drop(*drop)
    quarantine = (
        flagged.filter(F.col("_has_error"))
        .withColumn("failed_rules", F.concat_ws(",", *failed_ids))
        .withColumn("run_id", F.lit(run_id))
        .drop(*drop)
    )
    return valid, quarantine, results
