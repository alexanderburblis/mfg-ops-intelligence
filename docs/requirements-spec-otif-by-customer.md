# Requirements specification: OTIF by customer

*Example specification for the fictional Nordhalde Precision AG, written the way a BI analyst would hand a request to data engineering. It shows the path from a business question to a tested measure in this repository. It does not describe any real company.*

| | |
|---|---|
| Requested by | Head of Customer Service (fictional) |
| Business owner | Customer Service |
| Data owner | ERP / IT (sales order and packing slip data in D365) |
| Analyst | Alexander Burblis |
| Status | Implemented in `sm_mfg_ops`, report page *Delivery* and drill-through *Customer orders* |

## 1. Business request

> "Our key accounts complain about late deliveries. I need to see, per customer and per month, how many order lines we deliver on time and in full, which customers are below target, and the order lines behind the number."

## 2. Goal and decision supported

Identify customers with a delivery-performance problem early enough to act (expedite, re-plan, talk to the customer) and track whether actions work. The monthly operations review uses it; customer service uses the drill-through daily.

## 3. User stories

| ID | As a … | I want … | so that … |
|---|---|---|---|
| US-1 | customer service lead | OTIF % per customer for any period | I see which accounts are below target |
| US-2 | customer service lead | to open the order lines behind a customer's OTIF | I can call the customer with facts |
| US-3 | plant manager | the same KPI for my plant only | I am measured on what I control |
| US-4 | operations director | OTIF by month against the 90% target | I see the trend in the monthly review |
| US-5 | controller | customer segment as it was when the order was placed | segment results do not change when a customer is reclassified |

## 4. KPI definitions

| KPI | Definition | Grain | Measure |
|---|---|---|---|
| Shipped lines | Order lines with at least one packing slip line | order line | `[Shipped Lines]` |
| On time | Last shipment date ≤ requested receipt date | order line | `is_on_time` |
| In full | Total shipped quantity ≥ ordered quantity | order line | `is_in_full` |
| **OTIF %** | Shipped lines that are on time **and** in full ÷ shipped lines | ratio | `[OTIF %]` |
| OTIF gap | OTIF % − target (90%) | percentage points | `[OTIF Gap pts]` |
| Avg days late | Average of (last ship date − requested date) for late lines only | days | `[Avg Days Late (late lines)]` |

Rules and edge cases, agreed with the business owner:

1. **Multiple shipments per line**: the line counts as delivered on the date of its **last** shipment. A line shipped in two parts, the second one late, is late.
2. **Unshipped lines** are not in the denominator. They appear as open backlog (`[Open Order Backlog CHF]`), not as failures. Open question 1 below.
3. **Date used for the period**: order date (active relationship). Requested date is available as an inactive relationship for "due in month" views (`USERELATIONSHIP`).
4. **Requested receipt date** is used, not the confirmed date: the customer's expectation is what is measured.
5. **Customer segment** is taken from the customer version valid on the order date (slowly changing dimension type 2, `customer_sk`).
6. Lines rejected by data-quality rules (unknown customer, negative quantity, missing requested date) are **excluded** and listed on the *Data quality* page with owner and corrective action.

## 5. Source-to-target mapping

| D365 extract field (bronze) | Silver column | Gold column | Used for |
|---|---|---|---|
| SalesOrderLinesV2.SalesOrderNumber | so_id | fact_sales_order_line.so_id | line key, drill-through |
| SalesOrderLinesV2.LineCreationSequenceNumber | so_line | so_line | line key |
| SalesOrderLinesV2.OrderingCustomerAccountNumber (header field joined to the line) | customer_id | customer_id → customer_sk | customer, segment as of order date |
| SalesOrderLinesV2.RequestedReceiptDate | requested_date | requested_date | on-time test |
| SalesOrderLinesV2.OrderedSalesQuantity | order_qty | order_qty | in-full test |
| SalesPackingSlipLines.DeliveryDate | actual_ship_date | last_ship_date (max per line) | on-time test |
| SalesPackingSlipLines.DeliveredQuantity | shipped_qty | shipped_qty (sum per line) | in-full test |
| CustomersV3.SalesSegmentId | segment | dim_customer.segment (versioned) | segment analysis |

The full mapping of every extract, including which fields are standard D365 entity fields and which are custom or joined, is in [source-mapping.md](source-mapping.md).

## 6. Data-quality rules that protect this KPI

| Rule | Check | Severity | Owner |
|---|---|---|---|
| SO-01 | Customer exists in customer master | error (quarantine) | Customer service |
| SO-02 | Order quantity > 0 | error | Customer service |
| SO-03 | Requested date is filled | error | Customer service |
| SO-04 | Requested date not before order date | warning | Customer service |
| DL-01 | Delivery refers to an existing order line | error | Logistics |

If more than 2% of a table's rows fail error rules, the quality gate stops the run and the report keeps the last good data.

## 7. Non-functional requirements

| Area | Requirement | How it is met |
|---|---|---|
| Freshness | Data as of 06:00 every working day | Data Pipeline `pl_mfg_ops_daily`, incremental load of the change extract |
| Security | Plant managers see their plant only | RLS role *Plant access* (`gold_security_plant_access`, `USERPRINCIPALNAME()`) |
| Traceability | Every number traceable to the order line | Drill-through *Customer orders* |
| Reliability | Failed loads visible, report never shows half-loaded data | Quality gate, run log, *Pipeline runs* page, failure email from the pipeline |
| Correctness | KPI recalculated independently on every run | `reconcile.py` recomputes OTIF from silver and compares with gold |

## 8. Acceptance tests

| ID | Given | When | Then |
|---|---|---|---|
| AT-1 | A line ordered 10, shipped 10 on the requested date | the pipeline runs | the line is on time, in full, OTIF |
| AT-2 | A line ordered 10, shipped 6 on time and 4 two days late | the pipeline runs | last ship date is the late one, line is not OTIF |
| AT-3 | A line ordered 10, shipped 9 | the pipeline runs | not in full, not OTIF |
| AT-4 | A line with no shipment | the pipeline runs | not in the OTIF denominator, in open backlog |
| AT-5 | A customer moved from *Industrial* to *Semiconductor* on 15 Nov | orders before and after are reported by segment | older orders stay under *Industrial*, newer under *Semiconductor* (`test_sales_use_the_customer_version_valid_on_the_order_date`) |
| AT-6 | The full dataset | OTIF is recalculated from silver without gold flags | it equals the gold measure (reconciliation check, every run) |
| AT-7 | User `plant.manager.my01@nordhalde.example` | opens the *Delivery* page | only MY01 order lines are visible |

AT-5 and AT-6 run automatically (tests and reconciliation). AT-1 to AT-4 are covered by the flag logic in `pipeline._fact_sales` and checked against the recalculation. AT-7 is checked in the service with *Test as role*.

## 9. Open questions

1. Should lines that are overdue but not yet shipped count as failures in the month they fall due? (Common in practice. It changes the denominator; would be a second measure, "OTIF incl. overdue".)
2. Is the target the same for every segment? Research customers may accept longer lead times.
3. Should the confirmed date be measured as well (internal promise reliability) next to the requested date (customer expectation)?

## 10. Out of scope

Cancelled lines, returns, and carrier proof of delivery (arrival at the customer rather than ship date). They would need further D365 entities and a carrier data source.
