# Technical decision records

Short records of the main design choices: context, decision, alternatives, consequences.

## ADR-001 Lakehouse, not a warehouse, as the primary store

**Context.** Sources are ERP file extracts with quality problems. Some rows have to be kept but excluded (quarantine), and raw data must be replayable.
**Decision.** Fabric Lakehouse with Delta tables in three layers (bronze, silver, gold). Locally, Parquet folders with the same layout.
**Alternatives.** A Fabric Warehouse (T-SQL, full transactions) fits well when the team works mostly in SQL and the data arrives clean, or as a serving layer on top of gold. A plain warehouse with no raw layer loses the ability to replay a load after a rule changes.
**Consequences.** Transformations are written in PySpark, and gold can still be queried with T-SQL through the Lakehouse SQL analytics endpoint. Adding a Warehouse later for SQL-first consumers is possible without changing the pipeline.

## ADR-002 PySpark for the pipeline, SQL for checks, pandas for small utilities

**Context.** The brief asks where each tool fits rather than using one everywhere.
**Decision.**
- **PySpark** for bronze → silver → gold: it is the native engine of Fabric notebooks and scales from this sample (≈70k rows) to real ERP volumes (tens of millions of rows) without a rewrite.
- **Spark SQL** for reconciliation, because KPI definitions written as SQL are readable by analysts and independent of the DataFrame code they check.
- **pandas** only for generating test data and the demo, where the data is small and the code runs on one machine.
**Consequences.** At 70k rows, pandas would be faster locally. The choice is about the target platform and volume, and is documented as such.

## ADR-003 Quality gate before gold, not after

**Context.** Bad source data must not silently reach the report.
**Decision.** Rows failing an *error* rule always go to quarantine. If a table's error rate exceeds 2%, the run stops before gold is rebuilt, so the report keeps the last good data and the failure is visible in the run log.
**Alternatives.** Publishing the clean rows anyway (gold silently loses 5% of orders, so revenue and OTIF drop with no warning), or failing on any single bad row (one typo blocks the whole refresh).
**Consequences.** The threshold is a business decision. 2% is a starting value and would be agreed per table with the data owners.

## ADR-004 One Python package, thin notebooks

**Context.** Notebook code is hard to test and review.
**Decision.** All logic lives in the `mfgops` package. Notebooks only call it. CI tests the package and builds a wheel, which is installed into the Fabric Environment.
**Consequences.** The same code runs locally, in CI and in Fabric. Changing a rule is a reviewed, tested commit, not an edit in a notebook.

## ADR-005 Synthetic data with planted defects

**Context.** No real ERP data can be published.
**Decision.** A seeded generator produces realistic volumes and plants a known number of each defect. Tests assert that each one is caught.
**Consequences.** The data-quality framework is proven against known answers. The fictional company (Nordhalde Precision AG) does not represent any real company.
