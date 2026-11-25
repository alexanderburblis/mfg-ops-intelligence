"""Definition of the Direct Lake semantic model ``sm_mfg_ops`` as TMDL files.

Tables and columns come from ``GOLD_SCHEMA`` (the gold and meta tables as the
pipeline writes them; ``tools/build_fabric_items.py --capture-schema``
refreshes it from a local run, and a test checks it still matches).
Measures, relationships and row-level security roles are defined below.
``model_files()`` returns every file of the ``sm_mfg_ops.SemanticModel``
folder; ``mfgops.deploy`` publishes them and ``tools/build_fabric_items.py``
writes them to ``fabric/`` for review.

The SQL endpoint of the lakehouse is only known at deployment, so the
connection expression keeps two placeholders that the deploy step fills in.
"""

from __future__ import annotations

import json
import uuid

from mfgops.gold_schema import GOLD_SCHEMA

FOLDER = "sm_mfg_ops.SemanticModel"
NS = uuid.UUID("6f1c3a52-8f4e-4f63-9d1a-2b7c2d9e5a10")

SQL_ENDPOINT = "{{SQL_ENDPOINT}}"
SQL_ENDPOINT_ID = "{{SQL_ENDPOINT_ID}}"

TYPES = {
    "string": "string",
    "int": "int64",
    "bigint": "int64",
    "double": "double",
    "boolean": "boolean",
    "date": "dateTime",
    "timestamp": "dateTime",
}

# --------------------------------------------------------------------------- display settings
HIDDEN_SUFFIXES = ("_key", "_sk")
HIDDEN = {
    "gold_fact_sales_order_line": {"customer_id", "plant_id", "material_id"},
    "gold_fact_production_order": {"plant_id", "material_id", "work_center_id"},
    "gold_fact_inventory_snapshot": {"plant_id", "material_id"},
    "gold_fact_purchase_order": {"supplier_id", "material_id", "plant_id"},
    "gold_dim_date": {"month_no"},
}
HIDDEN_TABLES = {"gold_security_plant_access"}
SORT_BY = {("gold_dim_date", "month"): "month_no"}
FORMATS = {"date": "yyyy-mm-dd", "timestamp": "yyyy-mm-dd hh:nn", "decimal": "#,0.00", "double": "#,0.0"}
DESCRIPTIONS = {
    "gold_fact_sales_order_line": "One row per sales order line, with delivery outcome (on time, in full).",
    "gold_fact_production_order": "One row per production order, with planned and actual dates and yields.",
    "gold_fact_inventory_snapshot": "Weekly stock on hand per plant and item (snapshot fact, do not sum across dates).",
    "gold_fact_purchase_order": "One row per purchase order line, with promised and receipt dates.",
    "gold_dim_customer": "Customer versions (slowly changing dimension type 2). customer_sk is the version key.",
    "gold_dq_results": "One row per data-quality rule per pipeline run.",
    "meta_run_summary": "One row per pipeline run: mode, status, duration, volumes.",
    "meta_pipeline_runs": "One row per pipeline stage and attempt.",
    "gold_security_plant_access": "Row-level security: plants each user may see.",
}

# --------------------------------------------------------------------------- measures
S, P, INV, U, Q, R, X = (
    "gold_fact_sales_order_line",
    "gold_fact_production_order",
    "gold_fact_inventory_snapshot",
    "gold_fact_purchase_order",
    "gold_dq_results",
    "meta_run_summary",
    "meta_pipeline_runs",
)
PCT, CHF, NUM, DAYS = "0.0%", "#,0", "#,0", "#,0.0"
LAST_SNAP = f"VAR SnapDate = MAX ( {INV}[snapshot_date] ) RETURN"
LAST_DQ = f"VAR LastRun = MAXX ( ALL ( {Q} ), {Q}[run_id] ) RETURN"

MEASURES = [
    # table, name, expression, format, folder, description
    (S, "Revenue CHF", f"SUM ( {S}[order_value_chf] )", CHF, "Sales", "Order value of valid order lines."),
    (
        S,
        "Revenue PY CHF",
        "CALCULATE ( [Revenue CHF], SAMEPERIODLASTYEAR ( gold_dim_date[date] ) )",
        CHF,
        "Sales",
        "Same period last year (order date).",
    ),
    (S, "Revenue YoY %", "DIVIDE ( [Revenue CHF] - [Revenue PY CHF], [Revenue PY CHF] )", PCT, "Sales", ""),
    (S, "Order Lines", f"COUNTROWS ( {S} )", NUM, "Sales", ""),
    (S, "Customers with Orders", f"DISTINCTCOUNT ( {S}[customer_id] )", NUM, "Sales", ""),
    (
        S,
        "Shipped Lines",
        f"CALCULATE ( COUNTROWS ( {S} ), {S}[is_shipped] = TRUE () )",
        NUM,
        "Delivery",
        "",
    ),
    (
        S,
        "OTIF %",
        f"DIVIDE ( CALCULATE ( COUNTROWS ( {S} ), {S}[is_otif] = TRUE () ), [Shipped Lines] )",
        PCT,
        "Delivery",
        "Shipped lines delivered on or before the requested date and in full, divided by shipped lines.",
    ),
    (S, "OTIF Target %", "0.9", PCT, "Delivery", "Service-level target (assumption for the demo)."),
    (S, "OTIF Gap pts", "[OTIF %] - [OTIF Target %]", "+0.0%;-0.0%;0.0%", "Delivery", ""),
    (
        S,
        "On-Time %",
        f"DIVIDE ( CALCULATE ( COUNTROWS ( {S} ), {S}[is_on_time] = TRUE () ), [Shipped Lines] )",
        PCT,
        "Delivery",
        "",
    ),
    (
        S,
        "In-Full %",
        f"DIVIDE ( CALCULATE ( COUNTROWS ( {S} ), {S}[is_in_full] = TRUE (), {S}[is_shipped] = TRUE () ), [Shipped Lines] )",
        PCT,
        "Delivery",
        "",
    ),
    (
        S,
        "Avg Days Late (late lines)",
        f"CALCULATE ( AVERAGE ( {S}[days_late] ), {S}[days_late] > 0 )",
        DAYS,
        "Delivery",
        "",
    ),
    (
        S,
        "Open Order Backlog CHF",
        f"CALCULATE ( [Revenue CHF], {S}[is_shipped] = FALSE () )",
        CHF,
        "Delivery",
        "",
    ),
    (P, "Good Qty", f"SUM ( {P}[good_qty] )", NUM, "Production", ""),
    (P, "Scrap Qty", f"SUM ( {P}[scrap_qty] )", NUM, "Production", ""),
    (
        P,
        "Scrap Rate %",
        "DIVIDE ( [Scrap Qty], [Good Qty] + [Scrap Qty] )",
        "0.00%",
        "Production",
        "Scrap quantity divided by good plus scrap quantity of ended orders.",
    ),
    (P, "Scrap Cost CHF", f"SUM ( {P}[scrap_value_chf] )", CHF, "Production", "Scrap at standard cost."),
    (
        P,
        "Completed Orders",
        f"CALCULATE ( COUNTROWS ( {P} ), {P}[is_completed] = TRUE () )",
        NUM,
        "Production",
        "",
    ),
    (
        P,
        "Plan Attainment %",
        f"DIVIDE ( [Good Qty], CALCULATE ( SUM ( {P}[planned_qty] ), {P}[is_completed] = TRUE () ) )",
        PCT,
        "Production",
        "",
    ),
    (
        P,
        "Schedule Adherence %",
        f"DIVIDE ( CALCULATE ( COUNTROWS ( {P} ), {P}[is_on_schedule] = TRUE () ), [Completed Orders] )",
        PCT,
        "Production",
        "Ended orders finished on or before the planned finish date.",
    ),
    (P, "Avg Cycle Time (days)", f"AVERAGE ( {P}[cycle_days] )", DAYS, "Production", ""),
    (P, "Avg Start Delay (days)", f"AVERAGE ( {P}[start_delay_days] )", DAYS, "Production", ""),
    (
        INV,
        "Stock Value CHF",
        f"{LAST_SNAP} CALCULATE ( SUM ( {INV}[stock_value_chf] ), {INV}[snapshot_date] = SnapDate )",
        CHF,
        "Inventory",
        "Stock at standard cost on the last snapshot date in the filter context.",
    ),
    (
        INV,
        "Items Below Safety Stock",
        f"{LAST_SNAP} CALCULATE ( COUNTROWS ( {INV} ), {INV}[snapshot_date] = SnapDate, {INV}[is_below_safety_stock] = TRUE () )",
        NUM,
        "Inventory",
        "",
    ),
    (
        INV,
        "Median Days of Coverage",
        f"{LAST_SNAP} CALCULATE ( MEDIAN ( {INV}[days_of_coverage] ), {INV}[snapshot_date] = SnapDate )",
        DAYS,
        "Inventory",
        "On-hand quantity divided by average daily independent and dependent demand.",
    ),
    (
        U,
        "Supplier On-Time %",
        f"DIVIDE ( CALCULATE ( COUNTROWS ( {U} ), {U}[is_on_time] = TRUE () ), CALCULATE ( COUNTROWS ( {U} ), {U}[is_received] = TRUE () ) )",
        PCT,
        "Purchasing",
        "",
    ),
    (
        U,
        "Avg Supplier Days Late",
        f"CALCULATE ( AVERAGE ( {U}[days_late] ), {U}[days_late] > 0 )",
        DAYS,
        "Purchasing",
        "",
    ),
    (
        U,
        "Open Purchase Orders",
        f"CALCULATE ( COUNTROWS ( {U} ), {U}[is_received] = FALSE () )",
        NUM,
        "Purchasing",
        "",
    ),
    (Q, "Latest DQ Run", f"MAX ( {Q}[run_ts] )", "yyyy-mm-dd hh:nn", "Data quality", ""),
    (
        Q,
        "Rows Checked (latest run)",
        f"{LAST_DQ} CALCULATE ( SUM ( {Q}[rows_checked] ), {Q}[run_id] = LastRun )",
        NUM,
        "Data quality",
        "",
    ),
    (
        Q,
        "Rows Failed (latest run)",
        f"{LAST_DQ} CALCULATE ( SUM ( {Q}[rows_failed] ), {Q}[run_id] = LastRun )",
        NUM,
        "Data quality",
        "",
    ),
    (
        Q,
        "DQ Pass Rate %",
        "1 - DIVIDE ( [Rows Failed (latest run)], [Rows Checked (latest run)] )",
        "0.00%",
        "Data quality",
        "Share of rule checks that passed in the latest run.",
    ),
    (
        Q,
        "Rules Failing (latest run)",
        f"{LAST_DQ} CALCULATE ( COUNTROWS ( {Q} ), {Q}[run_id] = LastRun, {Q}[rows_failed] > 0 )",
        NUM,
        "Data quality",
        "",
    ),
    (
        Q,
        "Rows Failed",
        f"SUM ( {Q}[rows_failed] )",
        NUM,
        "Data quality",
        "Across the runs in the filter context.",
    ),
    (
        Q,
        "DQ Pass Rate per Run %",
        f"1 - DIVIDE ( SUM ( {Q}[rows_failed] ), SUM ( {Q}[rows_checked] ) )",
        "0.00%",
        "Data quality",
        "Pass rate for the runs in the filter context, for trends.",
    ),
    (R, "Runs", f"COUNTROWS ( {R} )", NUM, "Monitoring", ""),
    (R, "Failed Runs", f'CALCULATE ( COUNTROWS ( {R} ), {R}[status] = "failed" )', NUM, "Monitoring", ""),
    (R, "Run Success Rate %", "DIVIDE ( [Runs] - [Failed Runs], [Runs] )", PCT, "Monitoring", ""),
    (R, "Avg Run Duration (s)", f"AVERAGE ( {R}[seconds] )", "#,0", "Monitoring", ""),
    (
        R,
        "Last Run Status",
        f"VAR LastStart = MAX ( {R}[started_at] ) RETURN CALCULATE ( MAX ( {R}[status] ), {R}[started_at] = LastStart )",
        "",
        "Monitoring",
        "",
    ),
    (
        R,
        "Last Successful Run",
        f'CALCULATE ( MAX ( {R}[finished_at] ), {R}[status] = "success" )',
        "yyyy-mm-dd hh:nn",
        "Monitoring",
        "",
    ),
    (R, "Rows Quarantined", f"SUM ( {R}[rows_quarantined] )", NUM, "Monitoring", ""),
    (X, "Avg Stage Seconds", f"AVERAGE ( {X}[seconds] )", "#,0.0", "Monitoring", ""),
]

RELATIONSHIPS = [
    # name, from table, from col, to table, to col, active
    ("rel_sales_date", S, "order_date_key", "gold_dim_date", "date_key", True),
    ("rel_sales_requested_date", S, "requested_date_key", "gold_dim_date", "date_key", False),
    ("rel_sales_customer", S, "customer_sk", "gold_dim_customer", "customer_sk", True),
    ("rel_sales_material", S, "material_id", "gold_dim_material", "material_id", True),
    ("rel_sales_plant", S, "plant_id", "gold_dim_plant", "plant_id", True),
    ("rel_prod_date", P, "planned_finish_key", "gold_dim_date", "date_key", True),
    ("rel_prod_material", P, "material_id", "gold_dim_material", "material_id", True),
    ("rel_prod_plant", P, "plant_id", "gold_dim_plant", "plant_id", True),
    ("rel_prod_workcenter", P, "work_center_id", "gold_dim_work_center", "work_center_id", True),
    ("rel_inv_date", INV, "snapshot_date_key", "gold_dim_date", "date_key", True),
    ("rel_inv_material", INV, "material_id", "gold_dim_material", "material_id", True),
    ("rel_inv_plant", INV, "plant_id", "gold_dim_plant", "plant_id", True),
    ("rel_pur_date", U, "promised_date_key", "gold_dim_date", "date_key", True),
    ("rel_pur_supplier", U, "supplier_id", "gold_dim_supplier", "supplier_id", True),
    ("rel_pur_material", U, "material_id", "gold_dim_material", "material_id", True),
    ("rel_pur_plant", U, "plant_id", "gold_dim_plant", "plant_id", True),
]

RLS_FILTER = (
    "[plant_id] IN CALCULATETABLE ( VALUES ( gold_security_plant_access[plant_id] ), "
    "gold_security_plant_access[user_principal_name] = USERPRINCIPALNAME () )"
)
ROLES = {
    "Plant access": {
        "description": "Plant managers see their own plant. Users are listed in gold_security_plant_access.",
        "filters": {
            "gold_dim_plant": RLS_FILTER,
            "gold_security_plant_access": "[user_principal_name] = USERPRINCIPALNAME ()",
        },
    },
    "All plants": {"description": "Group management and controlling: no plant filter.", "filters": {}},
}


def tag(*parts: str) -> str:
    return str(uuid.uuid5(NS, "/".join(parts)))


def q(name: str) -> str:
    """Quote a TMDL object name when needed."""
    return name if name.replace("_", "").isalnum() else "'" + name.replace("'", "''") + "'"


def data_type(spark_type: str) -> str:
    return "decimal" if spark_type.startswith("decimal") else TYPES[spark_type]


def table_tmdl(name: str, columns: list[list[str]]) -> str:
    out = [f"table {name}", f"\tlineageTag: {tag(name)}"]
    if name in HIDDEN_TABLES:
        out.append("\tisHidden")
    if name == "gold_dim_date":
        out.append("\tdataCategory: Time")
    if name in DESCRIPTIONS:
        out.insert(0, f"/// {DESCRIPTIONS[name]}")
    out.append("")
    for m in [m for m in MEASURES if m[0] == name]:
        _, mname, expr, fmt, folder, desc = m
        if desc:
            out.append(f"\t/// {desc}")
        out.append(f"\tmeasure {q(mname)} = {expr}")
        if fmt:
            out.append(f"\t\tformatString: {fmt}")
        out.append(f"\t\tdisplayFolder: {folder}")
        out.append(f"\t\tlineageTag: {tag(name, 'm', mname)}")
        out.append("")
    for col, stype in columns:
        dt = data_type(stype)
        out.append(f"\tcolumn {q(col)}")
        out.append(f"\t\tdataType: {dt}")
        kind = "decimal" if stype.startswith("decimal") else stype
        if kind in FORMATS:
            out.append(f"\t\tformatString: {FORMATS[kind]}")
        if name == "gold_dim_date" and col == "date":
            out.append("\t\tisKey")
        if col.endswith(HIDDEN_SUFFIXES) or col in HIDDEN.get(name, set()):
            out.append("\t\tisHidden")
        out.append(f"\t\tlineageTag: {tag(name, col)}")
        numeric = dt in ("int64", "double", "decimal")
        is_id = col.endswith(("_id", "_key", "_sk", "_no", "year", "iso_week", "so_line", "attempt"))
        out.append(f"\t\tsummarizeBy: {'sum' if numeric and not is_id else 'none'}")
        out.append(f"\t\tsourceColumn: {col}")
        if (name, col) in SORT_BY:
            out.append(f"\t\tsortByColumn: {SORT_BY[(name, col)]}")
        out.append("")
        out.append("\t\tannotation SummarizationSetBy = Automatic")
        out.append("")
    out += [
        f"\tpartition {name} = entity",
        "\t\tmode: directLake",
        "\t\tsource",
        f"\t\t\tentityName: {name}",
        "\t\t\tschemaName: dbo",
        "\t\t\texpressionSource: DatabaseQuery",
        "",
    ]
    return "\n".join(out)


def model_files() -> dict[str, str]:
    """Relative path → file content for the whole semantic model folder."""
    files: dict[str, str] = {}
    files["definition.pbism"] = json.dumps({"version": "4.0", "settings": {}}, indent=2)
    files[".platform"] = json.dumps(
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "SemanticModel", "displayName": "sm_mfg_ops"},
            "config": {"version": "2.0", "logicalId": tag("sm_mfg_ops")},
        },
        indent=2,
    )
    files["definition/database.tmdl"] = "database\n\tcompatibilityLevel: 1604\n"
    model = [
        "model Model",
        "\tculture: en-US",
        "\tdefaultPowerBIDataSourceVersion: powerBI_V3",
        "\tdiscourageImplicitMeasures",
        "\tsourceQueryCulture: en-US",
        "\tdataAccessOptions",
        "\t\tlegacyRedirects",
        "\t\treturnErrorValuesAsNull",
        "",
    ]
    model += [f"ref table {t}" for t in GOLD_SCHEMA] + [""]
    model += [f"ref role {q(r)}" for r in ROLES] + [""]
    files["definition/model.tmdl"] = "\n".join(model)
    files["definition/expressions.tmdl"] = (
        "expression DatabaseQuery =\n"
        "\t\tlet\n"
        f'\t\t    database = Sql.Database("{SQL_ENDPOINT}", "{SQL_ENDPOINT_ID}")\n'
        "\t\tin\n"
        "\t\t    database\n"
        f"\tlineageTag: {tag('expr', 'DatabaseQuery')}\n\n"
        "\tannotation PBI_IncludeFutureArtifacts = False\n"
    )
    for t, cols in GOLD_SCHEMA.items():
        files[f"definition/tables/{t}.tmdl"] = table_tmdl(t, cols)
    rel = []
    for name, ft, fc, tt, tc, active in RELATIONSHIPS:
        rel += [f"relationship {name}"]
        if not active:
            rel += ["\tisActive: false"]
        rel += [f"\tfromColumn: {ft}.{fc}", f"\ttoColumn: {tt}.{tc}", ""]
    files["definition/relationships.tmdl"] = "\n".join(rel)
    for role, spec in ROLES.items():
        lines = [f"/// {spec['description']}", f"role {q(role)}", "\tmodelPermission: read", ""]
        for table, expr in spec["filters"].items():
            lines += [f"\ttablePermission {table} = {expr}", ""]
        files[f"definition/roles/{role}.tmdl"] = "\n".join(lines)
    return files
