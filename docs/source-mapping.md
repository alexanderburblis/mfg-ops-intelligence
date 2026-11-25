# Source mapping: D365-shaped extracts → canonical model

The fictional Nordhalde Precision AG runs Dynamics 365 Supply Chain Management. The generator writes its extracts with the entity and field names Microsoft publishes for D365 finance and operations data entities (Common Data Model definitions, github.com/microsoft/CDM, `core/operationsCommon/Entities`). Bronze keeps them unchanged; silver renames them to the canonical names below, in one place (`src/mfgops/source_map.py`).

45 of 64 mapped fields are fields of the named standard entity. The others (*Standard entity field = no*) would come from a joined header, a journal or a custom field in a real project; the note says which. Every extract also carries `ModifiedDateTime`, the change-tracking timestamp the incremental load uses as its watermark.

Nothing here describes any real company's D365 configuration.

| Canonical table | Extract file | D365 entity | Extract field | Canonical column | Standard entity field | Note |
|---|---|---|---|---|---|---|
| plants | OperationalSites.csv | InventOperationalSiteEntity | SiteId | plant_id | yes |  |
| plants | OperationalSites.csv | InventOperationalSiteEntity | SiteName | plant_name | yes |  |
| plants | OperationalSites.csv | InventOperationalSiteEntity | PrimaryAddressCountryRegionId | country | yes |  |
| plants | OperationalSites.csv | InventOperationalSiteEntity | CurrencyCode | currency | no | Accounting currency of the legal entity the site belongs to |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | ItemNumber | material_id | yes |  |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | SearchName | description | yes |  |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | ItemCategoryCode | material_type | no | FG / SFG / RM, from the product category hierarchy |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | ProductGroupId | product_family | yes |  |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | BOMUnitSymbol | base_uom | yes |  |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | UnitCost | standard_cost_chf | yes |  |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | SafetyStockQuantity | safety_stock_qty | no | Item coverage settings (minimum on hand) |
| materials | ReleasedProductsV2.csv | EcoResReleasedProductV2Entity | LeadTimeDays | lead_time_days | no | Default order settings, purchase lead time |
| bom | BillOfMaterialsLinesV3.csv | BOMBillOfMaterialsLineV3Entity | BOMId | parent_material_id | yes | Simplified: one active BOM per item, BOMId = parent item |
| bom | BillOfMaterialsLinesV3.csv | BOMBillOfMaterialsLineV3Entity | ItemNumber | component_material_id | yes |  |
| bom | BillOfMaterialsLinesV3.csv | BOMBillOfMaterialsLineV3Entity | Quantity | qty_per | yes |  |
| work_centers | OperationsResources.csv | OpResOperationsResourceEntity | ResourceId | work_center_id | yes |  |
| work_centers | OperationsResources.csv | OpResOperationsResourceEntity | SiteId | plant_id | no | From the resource group the resource is assigned to |
| work_centers | OperationsResources.csv | OpResOperationsResourceEntity | ResourceName | work_center_name | yes |  |
| work_centers | OperationsResources.csv | OpResOperationsResourceEntity | Capacity | capacity_hours_per_day | yes |  |
| customers | CustomersV3.csv | CustCustomerV3Entity | CustomerAccount | customer_id | yes |  |
| customers | CustomersV3.csv | CustCustomerV3Entity | OrganizationName | customer_name | yes |  |
| customers | CustomersV3.csv | CustCustomerV3Entity | SalesDistrict | region | yes |  |
| customers | CustomersV3.csv | CustCustomerV3Entity | SalesSegmentId | segment | yes |  |
| suppliers | VendorsV2.csv | VendVendorV2Entity | VendorAccountNumber | supplier_id | yes |  |
| suppliers | VendorsV2.csv | VendVendorV2Entity | VendorOrganizationName | supplier_name | yes |  |
| suppliers | VendorsV2.csv | VendVendorV2Entity | AddressCountryRegionId | country | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | SalesOrderNumber | so_id | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | LineCreationSequenceNumber | so_line | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | OrderingCustomerAccountNumber | customer_id | no | Header field (SalesOrderHeaderV2Entity), joined into the line extract |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | ItemNumber | material_id | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | ShippingSiteId | plant_id | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | OrderCreatedDate | order_date | no | Header creation date, joined into the line extract |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | RequestedReceiptDate | requested_date | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | OrderedSalesQuantity | order_qty | yes |  |
| sales_orders | SalesOrderLinesV2.csv | SalesOrderLineV2Entity | SalesPrice | net_price_chf | yes | In CHF, the group currency of this model |
| deliveries | SalesPackingSlipLines.csv | Packing slip journal lines (export view) | PackingSlipLineId | delivery_id | no |  |
| deliveries | SalesPackingSlipLines.csv | Packing slip journal lines (export view) | SalesOrderNumber | so_id | no |  |
| deliveries | SalesPackingSlipLines.csv | Packing slip journal lines (export view) | LineCreationSequenceNumber | so_line | no |  |
| deliveries | SalesPackingSlipLines.csv | Packing slip journal lines (export view) | DeliveryDate | actual_ship_date | no |  |
| deliveries | SalesPackingSlipLines.csv | Packing slip journal lines (export view) | DeliveredQuantity | shipped_qty | no |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ProductionOrderNumber | prod_order_id | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ProductionSiteId | plant_id | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ItemNumber | material_id | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ResourceId | work_center_id | no | Main resource of the route, joined from the route operation |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ScheduledStartDate | planned_start | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ScheduledEndDate | planned_finish | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | StartedDate | actual_start | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | EndedDate | actual_finish | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ScheduledQuantity | planned_qty | yes |  |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ReportedGoodQuantity | good_qty | no | Sum of report-as-finished journal, good quantity |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ReportedErrorQuantity | scrap_qty | no | Sum of report-as-finished journal, error quantity |
| production_orders | ProductionOrderHeaders.csv | ProdProductionOrderHeaderEntity | ProductionOrderStatus | status | yes |  |
| inventory_snapshots | InventorySiteOnHandV2.csv | InventorySiteOnHandV2Entity | SnapshotDate | snapshot_date | no | Added by the weekly export, the entity itself is current state |
| inventory_snapshots | InventorySiteOnHandV2.csv | InventorySiteOnHandV2Entity | InventorySiteId | plant_id | yes |  |
| inventory_snapshots | InventorySiteOnHandV2.csv | InventorySiteOnHandV2Entity | ItemNumber | material_id | yes |  |
| inventory_snapshots | InventorySiteOnHandV2.csv | InventorySiteOnHandV2Entity | OnHandQuantity | on_hand_qty | yes |  |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | PurchaseOrderNumber | purchase_order_id | yes |  |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | OrderVendorAccountNumber | supplier_id | no | Header field, joined into the line extract |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | ItemNumber | material_id | yes |  |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | ReceivingSiteId | plant_id | yes |  |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | OrderCreatedDate | order_date | no | Header creation date, joined into the line extract |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | ConfirmedDeliveryDate | promised_date | yes |  |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | ProductReceiptDate | received_date | no | From the product receipt journal |
| purchase_orders | PurchaseOrderLinesV2.csv | PurchPurchaseOrderLineV2Entity | OrderedPurchaseQuantity | order_qty | yes |  |
