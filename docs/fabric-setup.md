# Deploying to Microsoft Fabric

This is how the project was deployed on 25 November 2026. The free Fabric trial was not offered to the new tenant, so it runs on a pay-as-you-go **F2** capacity, paused when not in use.

## 0. Capacity (once)
1. Free Azure account, then a work account in Microsoft Entra ID (Fabric does not accept personal Microsoft accounts).
2. Give the work account the Contributor role on the subscription, and set up MFA.
3. As the work account, create a Fabric capacity in the Azure portal: size F2, overage protection off, capacity admin = the work account. **Pause it after every session** (Azure portal → capacity → Pause).

## Quick path used for the first deployment
Instead of uploading a wheel, the notebook installs the package straight from GitHub:

```
%pip install https://github.com/alexanderburblis/mfg-ops-intelligence/archive/refs/heads/main.zip --quiet
```

The Data Pipeline's notebook activity passes the base parameter `_inlineInstallationEnabled = true` (Bool), which allows `%pip` in scheduled runs. For production, pin a version (a tag or the CI wheel in an Environment) instead of `main`.

Relationships were added with the TMDL script in `semantic-model/relationships.tmdl` (TMDL view → Apply). Measures were added with `semantic-model/measures_deployed.dax` (DAX query view → Run → Update model with changes).

## 1. Workspace and lakehouse
1. In Fabric, create a workspace `mfg-ops-intelligence` on the trial capacity.
2. Create a lakehouse named `lh_mfg_ops`.

## 2. Environment with the package
1. Download the `mfgops-wheel` artifact from the latest GitHub Actions run (or run `python -m build --wheel` locally).
2. Create an environment `env_mfgops`, upload the `.whl` under **Custom libraries**, then publish.

## 3. Notebooks
1. Import `notebooks/00`–`04` (**Import → Notebook → From this computer**).
2. In each one, attach `lh_mfg_ops` as the default lakehouse and select `env_mfgops` as the environment.
3. Run `00_generate_source_data` once. It writes the source CSVs to `Files/raw`.

## 4. Pipeline (orchestration)
1. Create a Data Pipeline `pl_mfg_ops_daily`.
2. Add one **Notebook** activity running `04_orchestrated_run`. It runs all eight stages, writes `meta_pipeline_runs` and raises an error when the run fails, so the activity turns red.
3. On the activity's **On fail** path, add an **Office 365 Outlook** (or Teams) activity that sends the run ID and the error to the data owner.
4. Add a schedule (for example daily, 06:00).
5. Notebooks `01`–`03` stay available for running a single layer while debugging.

## 5. Semantic model and report
1. From the lakehouse, choose **New semantic model** and select the `gold_*` tables.
2. Create the relationships in `semantic-model/model.md`, mark `gold_dim_date` as the date table.
3. Add a `_Measures` table and paste the measures from `semantic-model/measures.dax`.
4. Build the six report pages listed in `model.md`.

## 6. Git integration
1. In the workspace settings, open **Git integration** and connect the GitHub repository, branch `main`, folder `/fabric`.
2. Commit the workspace items. Notebooks, pipeline, semantic model and report are then versioned next to the code.
