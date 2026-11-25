"""Build the Fabric notebooks (.ipynb) from the cell definitions below.

Keeping the notebooks generated means the logic lives in the tested
``mfgops`` package and the notebooks stay thin wrappers around it.
Run:  python tools/build_notebooks.py
"""

import json
from pathlib import Path

SETUP = """# Requires: this notebook attached to the lakehouse 'lh_mfg_ops' (default lakehouse)
# and the Fabric environment 'env_mfgops', which has the mfgops wheel from CI installed.
from mfgops.lakehouse import Lakehouse
lh = Lakehouse.detect(spark)
print("Running in Fabric:", lh.fabric)"""

NOTEBOOKS = {
    "00_generate_source_data": [
        (
            "markdown",
            "# 00 Generate source data\nWrites synthetic ERP/MRP extracts for the fictional Nordhalde Precision AG to `Files/raw` in the lakehouse. In a real setup this step is replaced by a pipeline copy activity from the ERP system.",
        ),
        (
            "code",
            "from mfgops.generate import generate\nplanted = generate('/lakehouse/default/Files/raw', seed=42)\nplanted",
        ),
    ],
    "01_bronze_ingest": [
        (
            "markdown",
            "# 01 Bronze: land raw extracts as Delta tables\nNo cleaning here. Every column stays text, plus load timestamp and source file, so the raw state can always be replayed.",
        ),
        ("code", SETUP),
        ("code", "from mfgops.pipeline import run_bronze\nrun_bronze(lh)"),
    ],
    "02_silver_quality": [
        (
            "markdown",
            "# 02 Silver: type, check, clean\nApplies the data-quality rules in `mfgops.quality`. Rows failing an *error* rule go to `quarantine_<table>`, results of every rule are appended to `gold_dq_results`.",
        ),
        ("code", SETUP),
        (
            "code",
            "from mfgops.pipeline import run_silver, new_run_id\nrun_id = new_run_id()\nsummary = run_silver(lh, run_id)\nsummary",
        ),
        ("code", "display(lh.read('gold', 'dq_results').filter(f\"run_id = '{run_id}'\").orderBy('rule_id'))"),
    ],
    "03_gold_star_schema": [
        (
            "markdown",
            "# 03 Gold: star schema for the semantic model\nBuilds `gold_dim_*` and `gold_fact_*` tables. The Power BI semantic model reads them in Direct Lake mode.",
        ),
        ("code", SETUP),
        ("code", "from mfgops.pipeline import run_gold\nrun_gold(lh)"),
    ],
}


def cell(kind, src):
    c = {"cell_type": kind, "metadata": {}, "source": src.splitlines(keepends=True)}
    if kind == "code":
        c.update(outputs=[], execution_count=None)
    return c


for name, cells in NOTEBOOKS.items():
    nb = {
        "cells": [cell(k, s) for k, s in cells],
        "metadata": {
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    Path("notebooks", f"{name}.ipynb").write_text(json.dumps(nb, indent=1))
    print("wrote", name)
