# Data-quality rules

Generated from `src/mfgops/quality.py` by `tools/gen_docs.py`. Do not edit by hand.

| Rule | Table | Dimension | Severity | Check | Owner | Corrective action |
|---|---|---|---|---|---|---|
| SO-01 | sales_orders | integrity | error | Customer exists in customer master | Customer service | Create or correct the customer in the ERP master data |
| SO-02 | sales_orders | validity | error | Order quantity is positive | Customer service | Correct the order line quantity |
| SO-03 | sales_orders | completeness | error | Requested delivery date is filled | Customer service | Enter the customer's requested date on the order |
| SO-04 | sales_orders | validity | warning | Requested date is not before order date | Customer service | Check the order dates with the customer |
| SO-05 | sales_orders | integrity | error | Plant code exists in the plant master | ERP / IT | Fix the plant mapping in the ERP extract |
| SO-06 | sales_orders | validity | error | Net price is positive | Pricing | Correct the price condition on the order |
| PR-01 | production_orders | uniqueness | error | Production order ID is unique | ERP / IT | Remove the duplicate from the extract and check the interface |
| PR-02 | production_orders | consistency | error | Actual finish is not before actual start | Production planning | Correct the confirmation dates |
| PR-03 | production_orders | validity | error | Good plus scrap quantity is plausible (max 150% of plan) | Production | Correct the yield or scrap confirmation |
| PR-04 | production_orders | integrity | warning | Work centre belongs to the same plant | Production planning | Check the routing |
| PR-05 | production_orders | validity | error | Status is a known D365 production status | ERP / IT | Map the new status in the extract |
| IN-01 | inventory_snapshots | validity | error | Stock on hand is not negative | Warehouse | Post the missing goods receipt or correct the stock |
| DL-01 | deliveries | integrity | error | Delivery refers to an existing sales order line | Logistics | Link the delivery to its order in the ERP |
| PU-01 | purchase_orders | consistency | warning | Goods receipt is not before the order date | Procurement | Check the goods receipt posting date |
| MA-01 | materials | validity | error | Item category is FG, SFG or RM | Master data | Correct the material type |
| MA-02 | materials | consistency | warning | Base unit of measure is PC (one unit across the model) | Master data | Add a unit conversion or correct the unit |
| PL-01 | plants | validity | warning | Plant currency is a supported currency | Finance | Add the currency to FX translation |
| TY-MAT | materials | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-BOM | bom | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-WOR | work_centers | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-SAL | sales_orders | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-DEL | deliveries | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-PRO | production_orders | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-INV | inventory_snapshots | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |
| TY-PUR | purchase_orders | type | error | Every typed column converts to its data type | ERP / IT | Fix the field format in the extract |

Plus the checks outside the rule engine:

| Check | Where | Effect |
|---|---|---|
| Source contract (file exists, exact columns, not empty) | `ingest.validate_sources` | Run fails before loading |
| Row count vs last successful run (warn ±30%, fail −50%) | `orchestrate.monitor_row_counts` | Warning or failed run |
| Error rate per table above 2% | `orchestrate` quality gate | Gold not refreshed |
| Silver vs gold totals, KPI recalculation | `reconcile.reconcile` | Failed run |
