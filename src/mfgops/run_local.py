"""Run the whole pipeline on this computer: generate data, then bronze, silver, gold.

Usage:
    python -m mfgops.run_local            # full run
    python -m mfgops.run_local --skip-generate
"""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path

from mfgops.generate import generate
from mfgops.lakehouse import Lakehouse, get_spark
from mfgops.pipeline import new_run_id, run_bronze, run_gold, run_silver


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--raw", default="data/raw")
    p.add_argument("--lakehouse", default="lakehouse")
    p.add_argument("--skip-generate", action="store_true")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()

    if not a.skip_generate:
        print("Planted defects:", generate(a.raw, a.seed))
    shutil.rmtree(a.lakehouse, ignore_errors=True)
    Path(a.lakehouse).mkdir(parents=True)

    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")
    lh = Lakehouse(spark, root=a.lakehouse, raw_path=a.raw)
    run_id = new_run_id()
    print("Bronze:", json.dumps(run_bronze(lh)))
    print("Silver:", json.dumps(run_silver(lh, run_id), indent=1))
    print("Gold:", json.dumps(run_gold(lh), indent=1))
    dq = lh.read("gold", "dq_results").orderBy("rule_id")
    dq.select("rule_id", "table_name", "severity", "rows_checked", "rows_failed").show(50, truncate=False)
    spark.stop()


if __name__ == "__main__":
    main()
