# KPI guide

For plant managers, supply chain and customer service. Each KPI: what it measures, how it is calculated, how to read it, and what to do when it moves.

| KPI | Definition | Formula | How to read it | When it drops: first questions |
|---|---|---|---|---|
| **OTIF %** | Share of shipped order lines delivered on time *and* in full | OTIF lines ÷ shipped lines | The customer's view of reliability. One late or short line counts as a miss | Which plant or segment? Late (capacity, material) or short (stock)? |
| **On-Time Delivery %** | Share of shipped lines shipped on or before the requested date | on-time lines ÷ shipped lines | Gap to OTIF = in-full problems | Check production schedule adherence and supplier on-time for the same period |
| **Scrap Rate %** | Scrapped units as a share of total output | scrap ÷ (good + scrap) | Quality cost of the process. Compare plants and work centres, not single orders | Which work centre? New material, new operator, machine change? |
| **Scrap Cost CHF** | Scrap valued at standard cost | Σ scrap qty × standard cost | Puts scrap in money. Prioritise by this, not by rate | Top 10 materials by scrap cost |
| **Plan Attainment %** | Good output against plan for completed orders | good qty ÷ planned qty | Below 100% means planned output was lost (scrap or short completion) | Is it scrap, or orders closed short? |
| **Schedule Adherence %** | Completed orders finished on or before planned finish | on-schedule ÷ completed | Drives OTIF two to four weeks later | Bottleneck work centre? Late material? |
| **Stock Value CHF** | Value of stock at the latest snapshot, at standard cost | Σ on hand × standard cost | Tied-up cash. Watch trend, not level | Which material type grew? Slow movers? |
| **Items Below Safety Stock** | Plant/material combinations under safety stock at latest snapshot | count | Shortage risk for production and delivery | Open purchase orders and their promised dates |
| **Days of Coverage** | Stock on hand ÷ average daily demand (sales + BOM-exploded production demand) | on hand ÷ avg daily demand | Under lead time = risk. Far above = excess | Compare with material lead time |
| **Supplier On-Time %** | Receipts on or before the promised date | on-time receipts ÷ receipts | Upstream reliability | Which suppliers? Escalate with data |
| **DQ Pass Rate %** | Rows passing all rules in the latest run | 1 − failed ÷ checked | Trust level of every other number | Open the data-quality page. Which rule and table? Fix at source |

## Reading the data-quality page
- **Error** rules remove rows from reporting and put them into quarantine. The KPIs above exclude them.
- **Warning** rules keep the row and only flag it.
- A rising count on one rule usually points to one process or interface. Name an owner, fix it in the ERP, then confirm it falls in the next run.
