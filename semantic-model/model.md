# Semantic model

Storage mode: **Direct Lake** on lakehouse `lh_mfg_ops` (no import refresh needed, reads the gold Delta tables directly).

## Relationships (all single direction, many-to-one)

| From (fact) | Column | To (dimension) | Column | Active |
|---|---|---|---|---|
| gold_fact_sales_order_line | order_date_key | gold_dim_date | date_key | yes |
| gold_fact_sales_order_line | requested_date_key | gold_dim_date | date_key | no (use `USERELATIONSHIP`) |
| gold_fact_sales_order_line | customer_id | gold_dim_customer | customer_id | yes |
| gold_fact_sales_order_line | material_id | gold_dim_material | material_id | yes |
| gold_fact_sales_order_line | plant_id | gold_dim_plant | plant_id | yes |
| gold_fact_production_order | planned_finish_key | gold_dim_date | date_key | yes |
| gold_fact_production_order | material_id | gold_dim_material | material_id | yes |
| gold_fact_production_order | plant_id | gold_dim_plant | plant_id | yes |
| gold_fact_production_order | work_center_id | gold_dim_work_center | work_center_id | yes |
| gold_fact_inventory_snapshot | snapshot_date_key | gold_dim_date | date_key | yes |
| gold_fact_inventory_snapshot | material_id | gold_dim_material | material_id | yes |
| gold_fact_inventory_snapshot | plant_id | gold_dim_plant | plant_id | yes |
| gold_fact_purchase_order | promised_date_key | gold_dim_date | date_key | yes |
| gold_fact_purchase_order | supplier_id | gold_dim_supplier | supplier_id | yes |
| gold_fact_purchase_order | material_id | gold_dim_material | material_id | yes |

`gold_dq_results` stands alone (no relationships). It feeds the data-quality page only.

## Modelling choices

- **Star schema**, one fact per business process (sales, production, inventory, purchasing). No fact-to-fact joins.
- **Inventory is a snapshot fact.** Its measures take the last snapshot in the filter context instead of summing across weeks.
- **Date keys are integers** (`yyyymmdd`), so Direct Lake relationships stay cheap.
- `gold_dim_date` is marked as the date table, so time-intelligence functions work.
- Technical columns (`*_key`, flags) are hidden, and users see measures and dimension attributes only.

## Report pages

1. **Overview**: revenue, OTIF, scrap rate, schedule adherence, stock value, supplier on-time, DQ pass rate.
2. **Delivery performance**: OTIF trend by plant and customer segment, late lines, backlog.
3. **Production**: scrap rate by plant and work centre, plan attainment, cycle time.
4. **Inventory & MRP**: stock value, items below safety stock, days of coverage by material type.
5. **Purchasing**: supplier on-time rate, days late distribution.
6. **Data quality**: pass rate over runs, failing rules by table and dimension, quarantined rows.
