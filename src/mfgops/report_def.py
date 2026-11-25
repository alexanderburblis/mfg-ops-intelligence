"""Build the Power BI report ``rpt_mfg_ops`` in PBIR format (Fabric Git / REST).

Nine pages on the ``sm_mfg_ops`` semantic model:

1. Overview            KPI cards, revenue against last year, OTIF and scrap by plant
2. Delivery            OTIF against target by month and segment, customers (drill-through)
3. Production          scrap rate and schedule adherence by plant, work centres (drill-through)
4. Inventory           stock value, items below safety stock, days of coverage
5. Purchasing          supplier on-time rate by month, country and supplier
6. Data quality        pass rate, failing rules with owner and action, trend by run
7. Pipeline runs       run history, success rate, duration per stage
8. Customer orders     drill-through: order lines of one customer
9. Work centre orders  drill-through: production orders of one work centre

``report_files()`` returns every file of the ``rpt_mfg_ops.Report`` folder.
``tools/build_fabric_items.py --validate <json-schemas clone>`` writes them to
``fabric/`` and validates every file against Microsoft's published PBIR JSON
schemas.
"""

from __future__ import annotations

import json
import uuid

FOLDER = "rpt_mfg_ops.Report"
NS = uuid.UUID("0b8a3e2f-5d7c-4c1e-9a64-3f2e8d1c7b55")
SCHEMA = "https://developer.microsoft.com/json-schemas/fabric/item/report/definition"
V_VISUAL, V_PAGE, V_REPORT = "2.4.0", "2.0.0", "3.0.0"

S, P, INV, U = (
    "gold_fact_sales_order_line",
    "gold_fact_production_order",
    "gold_fact_inventory_snapshot",
    "gold_fact_purchase_order",
)
Q, R, X = "gold_dq_results", "meta_run_summary", "meta_pipeline_runs"
D, PL, C, M, WC, SU = (
    "gold_dim_date",
    "gold_dim_plant",
    "gold_dim_customer",
    "gold_dim_material",
    "gold_dim_work_center",
    "gold_dim_supplier",
)

NAVY, TEAL, GREY, RED, AMBER, GREEN = "#17365D", "#2E7D9A", "#666666", "#B23A3A", "#D99A2B", "#3C8D5A"


# --------------------------------------------------------------------------- field helpers
def col(entity: str, prop: str) -> dict:
    return {"Column": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def meas(entity: str, prop: str) -> dict:
    return {"Measure": {"Expression": {"SourceRef": {"Entity": entity}}, "Property": prop}}


def proj(field: dict, display: str | None = None) -> dict:
    kind = "Column" if "Column" in field else "Measure"
    ent = field[kind]["Expression"]["SourceRef"]["Entity"]
    prop = field[kind]["Property"]
    p = {"field": field, "queryRef": f"{ent}.{prop}", "nativeQueryRef": prop}
    if display:
        p["displayName"] = display
    return p


def lit(v) -> dict:
    if isinstance(v, bool):
        v = "true" if v else "false"
    elif isinstance(v, (int, float)):
        v = f"{v}D"
    else:
        v = "'" + str(v).replace("'", "''") + "'"
    return {"expr": {"Literal": {"Value": v}}}


def color(hex_: str) -> dict:
    return {"solid": {"color": lit(hex_)}}


# --------------------------------------------------------------------------- visual builder
class Page:
    def __init__(self, name: str, display: str, drillthrough: dict | None = None):
        self.name, self.display, self.visuals, self.z = name, display, [], 0
        self.drillthrough = drillthrough

    def add(self, vtype, x, y, w, h, roles: dict, title=None, sort=None, objects=None, filters=None):
        self.z += 1
        vid = uuid.uuid5(NS, f"{self.name}/{self.z}").hex[:20]
        visual = {
            "visualType": vtype,
            "query": {"queryState": {r: {"projections": ps} for r, ps in roles.items()}},
            "drillFilterOtherVisuals": True,
        }
        if sort:
            field, direction = sort
            visual["query"]["sortDefinition"] = {
                "sort": [{"field": field, "direction": direction}],
                "isDefaultSort": True,
            }
        if objects:
            visual["objects"] = objects
        vco = {}
        if title:
            vco["title"] = [
                {
                    "properties": {
                        "show": lit(True),
                        "text": lit(title),
                        "fontColor": color(NAVY),
                        "fontSize": lit(11),
                        "bold": lit(True),
                    }
                }
            ]
        vco["background"] = [{"properties": {"show": lit(True), "color": color("#FFFFFF")}}]
        vco["border"] = [{"properties": {"show": lit(True), "color": color("#D9DEE5"), "radius": lit(4)}}]
        visual["visualContainerObjects"] = vco
        container = {
            "$schema": f"{SCHEMA}/visualContainer/{V_VISUAL}/schema.json",
            "name": vid,
            "position": {"x": x, "y": y, "z": self.z * 100, "width": w, "height": h, "tabOrder": self.z * 100},
            "visual": visual,
        }
        if filters:
            container["filterConfig"] = {"filters": filters}
        self.visuals.append(container)
        return self

    # convenience wrappers
    def card(self, x, y, w, h, field, title):
        label = {"color": color(NAVY), "fontSize": lit(22)}
        if "CHF" in field.get("Measure", {}).get("Property", ""):
            label["labelPrecision"] = lit(2)  # 1.82bn, 61.92M
        else:
            label["labelDisplayUnits"] = lit(1)  # counts and rates in full, no K/M
        objs = {
            "labels": [{"properties": label}],
            "categoryLabels": [{"properties": {"show": lit(False)}}],
        }
        return self.add("card", x, y, w, h, {"Values": [proj(field)]}, title=title, objects=objs)

    def slicer(self, x, y, w, h, field, title):
        objs = {
            "data": [{"properties": {"mode": lit("Dropdown")}}],
            "header": [{"properties": {"show": lit(False)}}],  # the container title names the slicer
        }
        return self.add("slicer", x, y, w, h, {"Values": [proj(field)]}, title=title, objects=objs)

    def bar(self, x, y, w, h, category, values, title, vtype="clusteredBarChart", sort=None, colors=None):
        objs = {"dataPoint": [{"properties": {"fill": color(colors or NAVY)}}]}
        objs["labels"] = [{"properties": {"show": lit(True), "color": color(GREY)}}]
        roles = {"Category": [proj(category)], "Y": [proj(v) for v in values]}
        return self.add(vtype, x, y, w, h, roles, title=title, sort=sort, objects=objs)

    def line(self, x, y, w, h, category, values, title, series=None):
        roles = {"Category": [proj(category)], "Y": [proj(v) for v in values]}
        if series:
            roles["Series"] = [proj(series)]
        sort = (category, "Ascending")
        return self.add("lineChart", x, y, w, h, roles, title=title, sort=sort)

    def table(self, x, y, w, h, fields, title, sort=None):
        objs = {
            "columnHeaders": [
                {"properties": {"fontColor": color("#FFFFFF"), "backColor": color(NAVY), "bold": lit(True)}}
            ],
            "values": [{"properties": {"fontSize": lit(9)}}],
        }
        roles = {"Values": [proj(f) for f in fields]}
        return self.add("tableEx", x, y, w, h, roles, title=title, sort=sort, objects=objs)


def header_slicers(pg: Page) -> None:
    pg.slicer(20, 12, 200, 56, col(PL, "plant_name"), "Plant")
    pg.slicer(230, 12, 160, 56, col(D, "year"), "Year")
    pg.slicer(400, 12, 200, 56, col(C, "segment"), "Customer segment")


def kpi_row(pg: Page, y: int, items: list[tuple[dict, str]]) -> None:
    n, gap, left, width = len(items), 12, 20, 1240
    w = (width - gap * (n - 1)) // n
    for i, (f, t) in enumerate(items):
        pg.card(left + i * (w + gap), y, w, 96, f, t)


# --------------------------------------------------------------------------- pages
def pages() -> list[Page]:
    out = []

    pg = Page("p01_overview", "Overview")
    header_slicers(pg)
    kpi_row(
        pg,
        80,
        [
            (meas(S, "Revenue CHF"), "Revenue (CHF)"),
            (meas(S, "OTIF %"), "OTIF"),
            (meas(P, "Scrap Rate %"), "Scrap rate"),
            (meas(P, "Schedule Adherence %"), "Schedule adherence"),
            (meas(INV, "Stock Value CHF"), "Stock value (CHF)"),
            (meas(U, "Supplier On-Time %"), "Supplier on-time"),
            (meas(Q, "DQ Pass Rate %"), "Data-quality pass rate"),
        ],
    )
    pg.line(
        20,
        190,
        820,
        250,
        col(D, "year_month"),
        [meas(S, "Revenue CHF"), meas(S, "Revenue PY CHF")],
        "Revenue by order month vs previous year",
    )
    pg.bar(
        852,
        190,
        408,
        250,
        col(PL, "plant_name"),
        [meas(S, "OTIF %")],
        "OTIF by plant",
        sort=(meas(S, "OTIF %"), "Ascending"),
    )
    pg.bar(20, 452, 408, 250, col(PL, "plant_name"), [meas(P, "Scrap Rate %")], "Scrap rate by plant", colors=RED)
    pg.bar(
        436,
        452,
        408,
        250,
        col(M, "product_family"),
        [meas(S, "Revenue CHF")],
        "Revenue by product family",
        sort=(meas(S, "Revenue CHF"), "Descending"),
        colors=TEAL,
    )
    pg.bar(
        852,
        452,
        408,
        250,
        col(C, "segment"),
        [meas(S, "Open Order Backlog CHF")],
        "Open order backlog by segment",
        sort=(meas(S, "Open Order Backlog CHF"), "Descending"),
        colors=GREY,
    )
    out.append(pg)

    pg = Page("p02_delivery", "Delivery")
    header_slicers(pg)
    kpi_row(
        pg,
        80,
        [
            (meas(S, "OTIF %"), "OTIF"),
            (meas(S, "OTIF Gap pts"), "Gap to 90% target"),
            (meas(S, "On-Time %"), "On time"),
            (meas(S, "In-Full %"), "In full"),
            (meas(S, "Avg Days Late (late lines)"), "Avg days late (late lines)"),
            (meas(S, "Open Order Backlog CHF"), "Open backlog (CHF)"),
        ],
    )
    pg.line(
        20,
        190,
        820,
        250,
        col(D, "year_month"),
        [meas(S, "OTIF %"), meas(S, "OTIF Target %")],
        "OTIF by order month against target",
    )
    pg.bar(
        852,
        190,
        408,
        250,
        col(C, "segment"),
        [meas(S, "OTIF %")],
        "OTIF by customer segment (segment at order date)",
        sort=(meas(S, "OTIF %"), "Ascending"),
        colors=TEAL,
    )
    pg.table(
        20,
        452,
        1240,
        250,
        [
            col(C, "customer_id"),
            col(C, "customer_name"),
            col(C, "segment"),
            col(C, "region"),
            meas(S, "Order Lines"),
            meas(S, "Revenue CHF"),
            meas(S, "OTIF %"),
            meas(S, "Avg Days Late (late lines)"),
        ],
        "Customers by order volume with OTIF and lateness (right-click a row → Drill through → Customer orders)",
        sort=(meas(S, "Order Lines"), "Descending"),
    )
    out.append(pg)

    pg = Page("p03_production", "Production")
    pg.slicer(20, 12, 200, 56, col(PL, "plant_name"), "Plant")
    pg.slicer(230, 12, 160, 56, col(D, "year"), "Year")
    pg.slicer(400, 12, 200, 56, col(M, "material_type"), "Item category")
    kpi_row(
        pg,
        80,
        [
            (meas(P, "Completed Orders"), "Completed orders"),
            (meas(P, "Scrap Rate %"), "Scrap rate"),
            (meas(P, "Scrap Cost CHF"), "Scrap cost (CHF)"),
            (meas(P, "Plan Attainment %"), "Plan attainment"),
            (meas(P, "Schedule Adherence %"), "Schedule adherence"),
            (meas(P, "Avg Cycle Time (days)"), "Avg cycle time (days)"),
        ],
    )
    pg.line(
        20,
        190,
        820,
        250,
        col(D, "year_month"),
        [meas(P, "Scrap Rate %")],
        "Scrap rate by planned finish month and plant",
        series=col(PL, "plant_name"),
    )
    pg.bar(
        852,
        190,
        408,
        250,
        col(PL, "plant_name"),
        [meas(P, "Schedule Adherence %")],
        "Schedule adherence by plant",
        sort=(meas(P, "Schedule Adherence %"), "Ascending"),
    )
    pg.table(
        20,
        452,
        1240,
        250,
        [
            col(WC, "work_center_id"),
            col(WC, "work_center_name"),
            meas(P, "Completed Orders"),
            meas(P, "Scrap Rate %"),
            meas(P, "Scrap Cost CHF"),
            meas(P, "Schedule Adherence %"),
            meas(P, "Avg Start Delay (days)"),
        ],
        "Work centres by scrap cost (right-click a row → Drill through → Work centre orders)",
        sort=(meas(P, "Scrap Cost CHF"), "Descending"),
    )
    out.append(pg)

    pg = Page("p04_inventory", "Inventory")
    pg.slicer(20, 12, 200, 56, col(PL, "plant_name"), "Plant")
    pg.slicer(230, 12, 200, 56, col(M, "material_type"), "Item category")
    pg.slicer(440, 12, 220, 56, col(M, "product_family"), "Product family")
    kpi_row(
        pg,
        80,
        [
            (meas(INV, "Stock Value CHF"), "Stock value (CHF), last snapshot"),
            (meas(INV, "Items Below Safety Stock"), "Items below safety stock"),
            (meas(INV, "Median Days of Coverage"), "Median days of coverage"),
        ],
    )
    pg.line(
        20,
        190,
        820,
        250,
        col(D, "year_month"),
        [meas(INV, "Stock Value CHF")],
        "Stock value, last weekly snapshot of each month, by plant",
        series=col(PL, "plant_name"),
    )
    pg.bar(
        852,
        190,
        408,
        250,
        col(M, "material_type"),
        [meas(INV, "Items Below Safety Stock")],
        "Items below safety stock by category",
        colors=AMBER,
    )
    pg.table(
        20,
        452,
        1240,
        250,
        [
            col(M, "material_id"),
            col(M, "description"),
            col(M, "material_type"),
            col(M, "safety_stock_qty"),
            meas(INV, "Stock Value CHF"),
            meas(INV, "Items Below Safety Stock"),
            meas(INV, "Median Days of Coverage"),
        ],
        "Items by stock value (days of coverage include BOM-dependent demand)",
        sort=(meas(INV, "Stock Value CHF"), "Descending"),
    )
    out.append(pg)

    pg = Page("p05_purchasing", "Purchasing")
    pg.slicer(20, 12, 200, 56, col(PL, "plant_name"), "Receiving plant")
    pg.slicer(230, 12, 160, 56, col(D, "year"), "Year")
    pg.slicer(400, 12, 200, 56, col(SU, "country"), "Supplier country")
    kpi_row(
        pg,
        80,
        [
            (meas(U, "Supplier On-Time %"), "Supplier on-time"),
            (meas(U, "Avg Supplier Days Late"), "Avg days late (late receipts)"),
            (meas(U, "Open Purchase Orders"), "Open purchase orders"),
        ],
    )
    pg.line(
        20,
        190,
        820,
        250,
        col(D, "year_month"),
        [meas(U, "Supplier On-Time %")],
        "Supplier on-time rate by promised month",
    )
    pg.bar(
        852,
        190,
        408,
        250,
        col(SU, "country"),
        [meas(U, "Supplier On-Time %")],
        "On-time rate by supplier country",
        sort=(meas(U, "Supplier On-Time %"), "Ascending"),
        colors=TEAL,
    )
    pg.table(
        20,
        452,
        1240,
        250,
        [
            col(SU, "supplier_id"),
            col(SU, "supplier_name"),
            col(SU, "country"),
            meas(U, "Supplier On-Time %"),
            meas(U, "Avg Supplier Days Late"),
            meas(U, "Open Purchase Orders"),
        ],
        "Suppliers, worst on-time rate first",
        sort=(meas(U, "Supplier On-Time %"), "Ascending"),
    )
    out.append(pg)

    pg = Page("p06_data_quality", "Data quality")
    kpi_row(
        pg,
        12,
        [
            (meas(Q, "DQ Pass Rate %"), "Pass rate, latest run"),
            (meas(Q, "Rows Checked (latest run)"), "Rule checks, latest run"),
            (meas(Q, "Rows Failed (latest run)"), "Failed checks, latest run"),
            (meas(Q, "Rules Failing (latest run)"), "Rules failing"),
            (meas(Q, "Latest DQ Run"), "Latest run"),
        ],
    )
    pg.table(
        20,
        120,
        820,
        330,
        [
            col(Q, "rule_id"),
            col(Q, "table_name"),
            col(Q, "dimension"),
            col(Q, "severity"),
            col(Q, "description"),
            col(Q, "owner"),
            col(Q, "corrective_action"),
            meas(Q, "Rows Failed"),
        ],
        "Rules with failures (all runs), owner and corrective action",
        sort=(meas(Q, "Rows Failed"), "Descending"),
    )
    pg.bar(
        852,
        120,
        408,
        330,
        col(Q, "dimension"),
        [meas(Q, "Rows Failed")],
        "Failed checks by quality dimension",
        sort=(meas(Q, "Rows Failed"), "Descending"),
        colors=RED,
    )
    pg.bar(
        20,
        462,
        1240,
        240,
        col(Q, "run_id"),
        [meas(Q, "DQ Pass Rate per Run %")],
        "Pass rate by pipeline run",
        vtype="clusteredColumnChart",
        sort=(col(Q, "run_id"), "Ascending"),
        colors=GREEN,
    )
    out.append(pg)

    pg = Page("p07_pipeline_runs", "Pipeline runs")
    kpi_row(
        pg,
        12,
        [
            (meas(R, "Last Run Status"), "Last run status"),
            (meas(R, "Last Successful Run"), "Last successful run"),
            (meas(R, "Runs"), "Runs"),
            (meas(R, "Run Success Rate %"), "Success rate"),
            (meas(R, "Avg Run Duration (s)"), "Avg duration (s)"),
            (meas(R, "Rows Quarantined"), "Rows quarantined"),
        ],
    )
    pg.table(
        20,
        120,
        1240,
        300,
        [
            col(R, "started_at"),
            col(R, "run_id"),
            col(R, "load_mode"),
            col(R, "status"),
            col(R, "failed_stage"),
            col(R, "seconds"),
            col(R, "rows_received"),
            col(R, "rows_quarantined"),
            col(R, "rows_inserted"),
            col(R, "rows_updated"),
            col(R, "message"),
        ],
        "Run history (newest first)",
        sort=(col(R, "started_at"), "Descending"),
    )
    pg.bar(
        20,
        432,
        620,
        270,
        col(X, "stage"),
        [meas(X, "Avg Stage Seconds")],
        "Average duration by stage (s)",
        sort=(meas(X, "Avg Stage Seconds"), "Descending"),
        colors=TEAL,
    )
    pg.bar(
        652,
        432,
        608,
        270,
        col(R, "load_mode"),
        [meas(R, "Avg Run Duration (s)")],
        "Average run duration by load mode (s)",
        vtype="clusteredColumnChart",
        colors=NAVY,
    )
    out.append(pg)

    drill_field = col(C, "customer_id")
    pg = Page("p08_customer_orders", "Customer orders", drillthrough={"field": drill_field})
    kpi_row(
        pg,
        12,
        [
            (meas(S, "Order Lines"), "Order lines"),
            (meas(S, "Revenue CHF"), "Revenue (CHF)"),
            (meas(S, "OTIF %"), "OTIF"),
            (meas(S, "Open Order Backlog CHF"), "Open backlog (CHF)"),
        ],
    )
    pg.table(
        20,
        120,
        1240,
        580,
        [
            col(S, "so_id"),
            col(S, "so_line"),
            col(D, "date"),
            col(S, "requested_date"),
            col(S, "last_ship_date"),
            col(M, "description"),
            col(PL, "plant_name"),
            col(S, "order_qty"),
            col(S, "shipped_qty"),
            col(S, "order_value_chf"),
            col(S, "is_otif"),
            col(S, "days_late"),
            col(C, "segment"),
        ],
        "Order lines of the selected customer (segment shown as valid on the order date)",
        sort=(col(D, "date"), "Descending"),
    )
    out.append(pg)

    pg = Page("p09_work_center_orders", "Work centre orders", drillthrough={"field": col(WC, "work_center_id")})
    kpi_row(
        pg,
        12,
        [
            (meas(P, "Completed Orders"), "Completed orders"),
            (meas(P, "Scrap Rate %"), "Scrap rate"),
            (meas(P, "Scrap Cost CHF"), "Scrap cost (CHF)"),
            (meas(P, "Schedule Adherence %"), "Schedule adherence"),
        ],
    )
    pg.table(
        20,
        120,
        1240,
        580,
        [
            col(P, "prod_order_id"),
            col(M, "material_id"),
            col(M, "description"),
            col(P, "status"),
            col(P, "planned_start"),
            col(P, "planned_finish"),
            col(P, "actual_start"),
            col(P, "actual_finish"),
            col(P, "planned_qty"),
            col(P, "good_qty"),
            col(P, "scrap_qty"),
            col(P, "scrap_value_chf"),
            col(P, "is_on_schedule"),
        ],
        "Production orders of the selected work centre",
        sort=(col(P, "scrap_value_chf"), "Descending"),
    )
    out.append(pg)
    return out


# --------------------------------------------------------------------------- files
THEME = {
    "name": "Nordhalde Navy",
    "dataColors": [NAVY, TEAL, "#8AA6C1", AMBER, GREEN, RED, GREY, "#C9D3DE"],
    "background": "#FFFFFF",
    "foreground": "#333333",
    "tableAccent": NAVY,
    "textClasses": {
        "title": {"fontFace": "Segoe UI Semibold", "color": NAVY, "fontSize": 12},
        "label": {"fontFace": "Segoe UI", "color": "#333333", "fontSize": 10},
        "callout": {"fontFace": "Segoe UI Semibold", "color": NAVY, "fontSize": 22},
    },
    "visualStyles": {"page": {"*": {"background": [{"color": {"solid": {"color": "#F4F6F9"}}, "transparency": 0}]}}},
}


def report_files() -> dict[str, str]:
    """Relative path → file content for the whole report folder."""
    files: dict[str, str] = {}

    def write(path: str, obj) -> None:
        files[path] = json.dumps(obj, indent=2, ensure_ascii=False)

    d = "definition"
    write(
        ".platform",
        {
            "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/platformProperties/2.0.0/schema.json",
            "metadata": {"type": "Report", "displayName": "rpt_mfg_ops"},
            "config": {"version": "2.0", "logicalId": str(uuid.uuid5(NS, "rpt_mfg_ops"))},
        },
    )
    # The semantic model id is filled in by mfgops.deploy (Fabric Git uses byPath instead).
    write(
        "definition.pbir",
        {
            "version": "4.0",
            "datasetReference": {
                "byConnection": {
                    "connectionString": "{{SEMANTIC_MODEL_CONNECTION}}",
                    "pbiServiceModelId": None,
                    "pbiModelVirtualServerName": "sobe_wowvirtualserver",
                    "pbiModelDatabaseName": "{{SEMANTIC_MODEL_ID}}",
                    "name": "EntityDataSource",
                    "connectionType": "pbiServiceXmlaStyleLive",
                }
            },
        },
    )
    write(f"{d}/version.json", {"$schema": f"{SCHEMA}/versionMetadata/1.0.0/schema.json", "version": "2.0.0"})
    write(
        "StaticResources/RegisteredResources/NordhaldeNavy.json",
        THEME,
    )
    write(
        f"{d}/report.json",
        {
            "$schema": f"{SCHEMA}/report/{V_REPORT}/schema.json",
            "themeCollection": {
                "customTheme": {
                    "name": "NordhaldeNavy.json",
                    "reportVersionAtImport": {"visual": "1.8.0", "report": "2.0.0", "page": "1.3.0"},
                    "type": "RegisteredResources",
                }
            },
            "resourcePackages": [
                {
                    "name": "RegisteredResources",
                    "type": "RegisteredResources",
                    "items": [{"name": "NordhaldeNavy.json", "path": "NordhaldeNavy.json", "type": "CustomTheme"}],
                }
            ],
            "settings": {
                "useStylableVisualContainerHeader": True,
                "defaultDrillFilterOtherVisuals": True,
                "allowChangeFilterTypes": True,
                "useEnhancedTooltips": True,
            },
        },
    )
    all_pages = pages()
    write(
        f"{d}/pages/pages.json",
        {
            "$schema": f"{SCHEMA}/pagesMetadata/1.0.0/schema.json",
            "pageOrder": [p.name for p in all_pages],
            "activePageName": all_pages[0].name,
        },
    )
    for pg in all_pages:
        page = {
            "$schema": f"{SCHEMA}/page/{V_PAGE}/schema.json",
            "name": pg.name,
            "displayName": pg.display,
            "displayOption": "FitToPage",
            "height": 720,
            "width": 1280,
        }
        if pg.drillthrough:
            fname = "drill_" + pg.name
            page["type"] = "Drillthrough"
            page["visibility"] = "HiddenInViewMode"
            page["pageBinding"] = {
                "name": "binding_" + pg.name,
                "type": "Drillthrough",
                "parameters": [{"name": "param_customer", "boundFilter": fname, "fieldExpr": pg.drillthrough["field"]}],
            }
            page["filterConfig"] = {
                "filters": [
                    {
                        "name": fname,
                        "field": pg.drillthrough["field"],
                        "type": "Categorical",
                        "howCreated": "Drillthrough",
                    }
                ]
            }
        write(f"{d}/pages/{pg.name}/page.json", page)
        for v in pg.visuals:
            write(f"{d}/pages/{pg.name}/visuals/{v['name']}/visual.json", v)
    return files
