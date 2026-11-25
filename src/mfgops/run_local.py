"""Generate fresh source data and run the orchestrated pipeline on this computer.

Usage:
    python -m mfgops.run_local                 # clean lakehouse, generate, run
    python -m mfgops.run_local --skip-generate # rerun on the existing extracts
    python -m mfgops.run_local --with-increment # full load, then the 15 Nov change extract
"""

from __future__ import annotations

import argparse
import json
import shutil

from mfgops.generate import generate, generate_increment
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.orchestrate import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--lakehouse", default="lakehouse")
    p.add_argument("--skip-generate", action="store_true")
    p.add_argument("--keep-lakehouse", action="store_true", help="keep run history from earlier runs")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--with-increment", action="store_true", help="then load the 15 Nov 2026 change extract")
    p.add_argument("--increment-dir", default="data/increments/2026-11-15")
    a = p.parse_args()

    if not a.skip_generate:
        print("Planted defects:", generate(a.raw, a.seed))
    if not a.keep_lakehouse:
        shutil.rmtree(a.lakehouse, ignore_errors=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=a.lakehouse, raw_path=a.raw)
    res = run_pipeline(lh)
    print(json.dumps({"run_id": res.run_id, "status": res.status, "failed_stage": res.failed_stage}, indent=1))
    if res.status == "success":
        lh.read("gold", "dq_results").filter("rows_failed > 0").select(
            "rule_id", "table_name", "severity", "rows_failed", "owner"
        ).orderBy("rule_id").show(50, truncate=False)
        for c in res.outputs["reconciliation"]:
            print(("PASS " if c["passed"] else "FAIL ") + c["check"])
    if a.with_increment and res.status == "success":
        print("Change extract:", generate_increment(a.increment_dir, a.seed))
        lh.raw_path = a.increment_dir
        inc = run_pipeline(lh, mode="incremental")
        print(json.dumps({"run_id": inc.run_id, "mode": inc.mode, "status": inc.status, "message": inc.message}))
        hist = lh.read("silver", "customers_history")
        moved = hist.filter("NOT is_current").select("customer_id")
        print("Customer history for customers that changed segment (SCD type 2):")
        hist.join(moved, "customer_id").orderBy("customer_id", "valid_from").show(20, truncate=False)
        lh.read("meta", "run_summary").select(
            "run_id",
            "load_mode",
            "status",
            "seconds",
            "rows_received",
            "rows_quarantined",
            "rows_inserted",
            "rows_updated",
        ).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
