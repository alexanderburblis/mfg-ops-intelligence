# Deploying to Microsoft Fabric

## 1. Workspace and lakehouse
1. In Fabric, create a workspace `mfg-ops-intelligence` on the trial capacity.
2. Create a lakehouse named `lh_mfg_ops`.

## 2. Environment with the package
1. Download the `mfgops-wheel` artifact from the latest GitHub Actions run (or run `python -m build --wheel` locally).
2. Create an environment `env_mfgops`, upload the `.whl` under **Custom libraries**, then publish.

## 3. Notebooks
1. Import `notebooks/00`–`03` (**Import → Notebook → From this computer**).
2. In each one, attach `lh_mfg_ops` as the default lakehouse and select `env_mfgops` as the environment.
3. Run `00_generate_source_data` once. It writes the source CSVs to `Files/raw`.

## 4. Pipeline (orchestration)
1. Create a Data Pipeline `pl_mfg_ops_daily`.
2. Add three **Notebook** activities: `01_bronze_ingest` → `02_silver_quality` → `03_gold_star_schema`, each running *on success* of the one before.
3. Add a schedule (for example daily, 06:00).
4. Optional: add a failure path, an Office 365 Outlook activity that emails the data owner.

## 5. Semantic model and report
1. From the lakehouse, choose **New semantic model** and select the `gold_*` tables.
2. Create the relationships in `semantic-model/model.md`, mark `gold_dim_date` as the date table.
3. Add a `_Measures` table and paste the measures from `semantic-model/measures.dax`.
4. Build the six report pages listed in `model.md`.

## 6. Git integration
1. In the workspace settings, open **Git integration** and connect the GitHub repository, branch `main`, folder `/fabric`.
2. Commit the workspace items. Notebooks, pipeline, semantic model and report are then versioned next to the code.
