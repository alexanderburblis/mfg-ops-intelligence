"""Build the Fabric notebooks (.ipynb) from the cell definitions below.

Keeping the notebooks generated means the logic lives in the tested
``mfgops`` package and the notebooks stay thin wrappers around it.
Run:  python tools/build_notebooks.py
"""

import json
from pathlib import Path

INSTALL = "%pip install https://github.com/alexanderburblis/mfg-ops-intelligence/archive/refs/heads/main.zip --quiet"

SETUP = """# Requires: this notebook attached to the lakehouse 'lh_mfg_ops' (default lakehouse).
# The mfgops package is installed by the %pip cell above (pin a tag or use an Environment in production).
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
            INSTALL,
        ),
        (
            "code",
            "from mfgops.generate import generate\nplanted = generate('/lakehouse/default/Files/raw', seed=42)\nplanted",
        ),
    ],
    "05_generate_increment": [
        (
            "markdown",
            "# 05 Generate the change extract\nWrites the 15 Nov 2026 delivery (new and changed rows only, D365 entity names, `ModifiedDateTime`) to `Files/increments/2026-11-15`. Run `04_orchestrated_run` with `load_mode = 'incremental'` afterwards.",
        ),
        ("code", INSTALL),
        (
            "code",
            "from mfgops.generate import generate_increment\ngenerate_increment('/lakehouse/default/Files/increments/2026-11-15', seed=42)",
        ),
    ],
    "01_bronze_ingest": [
        (
            "markdown",
            "# 01 Bronze: land raw extracts as Delta tables\nNo cleaning here. Every column stays text, plus load timestamp and source file, so the raw state can always be replayed.",
        ),
        ("code", INSTALL),
        ("code", SETUP),
        ("code", "from mfgops.pipeline import run_bronze\nrun_bronze(lh)"),
    ],
    "02_silver_quality": [
        (
            "markdown",
            "# 02 Silver: type, check, clean\nApplies the data-quality rules in `mfgops.quality`. Rows failing an *error* rule go to `quarantine_<table>`, results of every rule are appended to `gold_dq_results`.",
        ),
        ("code", INSTALL),
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
        ("code", INSTALL),
        ("code", SETUP),
        ("code", "from mfgops.pipeline import run_gold\nrun_gold(lh)"),
    ],
    "04_orchestrated_run": [
        (
            "markdown",
            "# 04 Orchestrated run (scheduled by the Fabric Data Pipeline)\nRuns all stages in order with logging, retries, the quality gate and reconciliation. Raises an error when the run fails, so the pipeline activity turns red and its failure path (alert email) fires.",
        ),
        (
            "parameters",
            '# Parameters (the Data Pipeline overrides these)\nload_mode = "full"  # or "incremental"\nraw_path = "Files/raw"  # e.g. "Files/increments/2026-11-15" for a change extract\ngenerate_sources = True  # demo only: write synthetic extracts first (no real ERP connected)',
        ),
        ("code", INSTALL),
        (
            "code",
            'from mfgops.generate import generate, generate_increment\nfrom mfgops.lakehouse import Lakehouse\nfrom mfgops.orchestrate import run_pipeline\n\nif generate_sources:\n    target = "/lakehouse/default/" + raw_path\n    print(generate_increment(target) if load_mode == "incremental" else generate(target))\nlh = Lakehouse.detect(spark)\nlh.raw_path = raw_path\nres = run_pipeline(lh, max_error_rate=0.02, mode=load_mode)\nprint(res.run_id, res.mode, res.status, res.failed_stage, res.message)\nif res.status != "success":\n    raise RuntimeError(f"Pipeline run {res.run_id} failed at {res.failed_stage}: {res.message}")',
        ),
    ],
    "06_deploy_model_and_report": [
        (
            "markdown",
            "# 06 Deploy semantic model and report (deployment as code)\nPublishes `sm_mfg_ops` (Direct Lake, TMDL) and `rpt_mfg_ops` (PBIR) from the definitions in `mfgops.model_def` and `mfgops.report_def` through the Fabric REST API, fills in this workspace's SQL endpoint and refreshes the model. Run it in the test workspace, check, then run it in production.",
        ),
        ("code", INSTALL),
        ("code", "from mfgops.deploy import deploy\nids = deploy()\nids"),
    ],
}


def cell(kind, src):
    meta = {}
    if kind == "parameters":  # a code cell tagged as the notebook's parameter cell
        kind, meta = "code", {"tags": ["parameters"]}
    c = {"cell_type": kind, "metadata": meta, "source": src.splitlines(keepends=True)}
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
