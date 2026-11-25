"""Source-to-target reconciliation and independent KPI recalculation.

Two kinds of check, run after gold is built:

1. Totals: quantities and counts in gold must equal those in silver.
2. KPIs: OTIF and scrap rate are recalculated in Spark SQL straight from the
   silver tables, without using any of the flags the gold step computed, and
   compared with the result from the gold tables. A modelling change that
   alters a KPI by accident shows up here.
"""

from __future__ import annotations

from mfgops.lakehouse import Lakehouse

TOLERANCE = 1e-9


def _scalar(lh: Lakehouse, sql: str):
    return lh.spark.sql(sql).first()[0]


def reconcile(lh: Lakehouse) -> list[dict]:
    for layer, names in {
        "silver": ["sales_orders", "deliveries", "production_orders", "inventory_snapshots", "materials"],
        "gold": ["fact_sales_order_line", "fact_production_order", "fact_inventory_snapshot"],
    }.items():
        for n in names:
            lh.read(layer, n).createOrReplaceTempView(f"{layer}_{n}")

    checks = [
        (
            "Sales order quantity, silver vs gold",
            "SELECT SUM(order_qty) FROM silver_sales_orders",
            "SELECT SUM(order_qty) FROM gold_fact_sales_order_line",
        ),
        (
            "Production good quantity, silver vs gold",
            "SELECT SUM(good_qty) FROM silver_production_orders",
            "SELECT SUM(good_qty) FROM gold_fact_production_order",
        ),
        (
            "Inventory snapshot rows, silver vs gold",
            "SELECT COUNT(*) FROM silver_inventory_snapshots",
            "SELECT COUNT(*) FROM gold_fact_inventory_snapshot",
        ),
        (
            "OTIF %, recalculated from silver vs gold flags",
            """
            WITH d AS (SELECT so_id, so_line, MAX(actual_ship_date) AS ship, SUM(shipped_qty) AS qty
                       FROM silver_deliveries GROUP BY so_id, so_line)
            SELECT AVG(CASE WHEN d.ship <= s.requested_date AND d.qty >= s.order_qty THEN 1.0 ELSE 0.0 END)
            FROM silver_sales_orders s JOIN d USING (so_id, so_line)
            """,
            "SELECT AVG(CASE WHEN is_otif THEN 1.0 ELSE 0.0 END) FROM gold_fact_sales_order_line WHERE is_shipped",
        ),
        (
            "Scrap rate %, recalculated from silver vs gold",
            "SELECT SUM(scrap_qty) / SUM(good_qty + scrap_qty) FROM silver_production_orders WHERE status = 'TECO'",
            "SELECT SUM(scrap_qty) / SUM(output_qty) FROM gold_fact_production_order WHERE is_completed",
        ),
    ]
    results = []
    for name, src_sql, tgt_sql in checks:
        src, tgt = _scalar(lh, src_sql), _scalar(lh, tgt_sql)
        ok = (
            src is not None
            and tgt is not None
            and abs(float(src) - float(tgt)) <= TOLERANCE * max(1.0, abs(float(src)))
        )
        results.append({"check": name, "source": float(src or 0), "target": float(tgt or 0), "passed": bool(ok)})
    return results
