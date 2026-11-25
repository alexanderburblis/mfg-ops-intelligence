"""Generate fresh source data and run the orchestrated pipeline on this computer.

Usage:
    python -m mfgops.run_local                 # clean lakehouse, generate, run
    python -m mfgops.run_local --skip-generate # rerun on the existing extracts
"""

from __future__ import annotations

import argparse
import json
import shutil

from mfgops.generate import generate
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.orchestrate import run_pipeline


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--lakehouse", default="lakehouse")
    p.add_argument("--skip-generate", action="store_true")
    p.add_argument("--keep-lakehouse", action="store_true", help="keep run history from earlier runs")
    p.add_argument("--seed", type=int, default=42)
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
    spark.stop()


if __name__ == "__main__":
    main()
