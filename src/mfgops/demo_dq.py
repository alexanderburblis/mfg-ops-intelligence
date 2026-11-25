"""Data-quality demonstration: a defective extract is caught, fixed and reprocessed.

Walks through six steps and prints what happens at each:

  1. Baseline: clean extracts, the pipeline runs green.
  2. Defective input: the next sales-order extract arrives with an unknown
     plant code on 5% of lines (a broken plant mapping in the ERP interface).
  3. Failed validation: rule SO-05 fails, the quality gate stops the run
     before gold is refreshed, so the report keeps the last good data.
  4. Diagnostics: failing rules with owner and corrective action, and sample
     rows from quarantine.
  5. Corrective action: the mapping is fixed and the extract resent.
  6. Successful rerun: the gate passes, gold is refreshed, reconciliation passes.

Bonus: an extract with a missing column fails at source validation, before
anything is loaded.

Usage:
    python -m mfgops.demo_dq
"""

from __future__ import annotations

import shutil
from pathlib import Path

import numpy as np
import pandas as pd

from mfgops.generate import generate
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.orchestrate import run_pipeline

RAW = Path("data/demo_raw")
LAKE = "lakehouse_demo"


def banner(n, text):
    print(f"\n{'=' * 78}\nSTEP {n}: {text}\n{'=' * 78}")


def last_gold_refresh(lh: Lakehouse) -> str:
    runs = lh.read("meta", "pipeline_runs").filter("stage = 'gold' AND status = 'success'")
    row = runs.orderBy(runs.finished_at.desc()).first()
    return row["run_id"] if row else "never"


def main() -> None:
    shutil.rmtree(RAW, ignore_errors=True)
    shutil.rmtree(LAKE, ignore_errors=True)
    generate(RAW, seed=42)
    spark = get_spark("mfgops-demo")
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=LAKE, raw_path=str(RAW))

    banner(1, "Baseline run on clean extracts")
    r1 = run_pipeline(lh)
    print(f"→ run {r1.run_id}: {r1.status.upper()}. Gold last refreshed by {last_gold_refresh(lh)}")

    banner(2, "Defective input: 5% of sales-order lines arrive with plant code 'DE01'")
    so_path = RAW / "SalesOrderLinesV2.csv"
    good_copy = RAW / "SalesOrderLinesV2.good.bak"
    shutil.copy(so_path, good_copy)
    so = pd.read_csv(so_path, dtype=str)
    rng = np.random.default_rng(1)
    idx = rng.choice(so.index, size=int(len(so) * 0.05), replace=False)
    so.loc[idx, "ShippingSiteId"] = "DE01"
    so.to_csv(so_path, index=False)
    print(f"→ {len(idx)} of {len(so)} lines now carry plant 'DE01', which is not in the plant master")
    cols = ["SalesOrderNumber", "LineCreationSequenceNumber", "OrderingCustomerAccountNumber", "ShippingSiteId"]
    print(so.loc[idx[:3], cols].to_string(index=False))

    banner(3, "Pipeline run: validation fails, the gate blocks the gold refresh")
    r2 = run_pipeline(lh)
    print(f"→ run {r2.run_id}: {r2.status.upper()} at stage '{r2.failed_stage}'")
    print(f"→ reason: {r2.message}")
    print(f"→ gold last refreshed by {last_gold_refresh(lh)} (the baseline run, so the report is unaffected)")

    banner(4, "Diagnostics: what failed, who owns it, what to do")
    lh.read("gold", "dq_results").filter(f"run_id = '{r2.run_id}' AND rows_failed > 0").select(
        "rule_id", "table_name", "rows_failed", "rows_checked", "owner", "corrective_action"
    ).orderBy("rows_failed", ascending=False).show(truncate=False)
    print("Sample of quarantined rows:")
    lh.read("quarantine", "sales_orders").filter("failed_rules LIKE '%SO-05%'").select(
        "so_id", "so_line", "plant_id", "failed_rules"
    ).show(5, truncate=False)

    banner(5, "Corrective action: ERP / IT fixes the plant mapping and resends the extract")
    shutil.move(good_copy, so_path)
    print("→ SalesOrderLinesV2.csv replaced with the corrected extract")

    banner(6, "Rerun: gate passes, gold refreshed, reconciliation green")
    r3 = run_pipeline(lh)
    print(f"→ run {r3.run_id}: {r3.status.upper()}. Gold last refreshed by {last_gold_refresh(lh)}")
    for c in r3.outputs.get("reconciliation", []):
        print(("  PASS " if c["passed"] else "  FAIL ") + c["check"])

    banner("+", "Bonus: an extract arrives with a column missing")
    pd.read_csv(RAW / "SalesPackingSlipLines.csv", dtype=str).drop(columns=["DeliveredQuantity"]).to_csv(
        RAW / "SalesPackingSlipLines.csv", index=False
    )
    r4 = run_pipeline(lh)
    print(f"→ run {r4.run_id}: {r4.status.upper()} at stage '{r4.failed_stage}': nothing was loaded")
    print(f"→ {r4.message}")

    print("\nRun history (meta_pipeline_runs):")
    lh.read("meta", "pipeline_runs").groupBy("run_id").agg({"stage": "count", "finished_at": "max"}).orderBy(
        "max(finished_at)"
    ).show(truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
