"""Source mapping: ERP extracts shaped like Microsoft Dynamics 365 data entities.

The fictional Nordhalde Precision AG runs Dynamics 365 Supply Chain Management.
Its extracts use the entity and field names that Microsoft publishes for D365
finance and operations data entities (Common Data Model definitions,
github.com/microsoft/CDM, ``core/operationsCommon/Entities``). Bronze keeps
those names exactly as delivered. Silver renames them to the canonical model
names used by every rule, fact and measure, so a change in the source only
touches this file.

Not every column a manufacturing report needs is a field on the standard
entity. Those are marked ``standard=False``: in a real project they come from
a second entity, a join in the export, or a custom field, and the mapping
document says which. Nothing here describes any real company's D365 setup.

``ModifiedDateTime`` is on every extract. It is the change-tracking timestamp
the incremental load uses as its watermark.
"""

from __future__ import annotations

from dataclasses import dataclass, field

WATERMARK_COLUMN = "ModifiedDateTime"


@dataclass(frozen=True)
class Field:
    source: str  # column name in the extract
    target: str  # canonical column name in silver
    standard: bool = True  # True: field exists on the named D365 entity
    note: str = ""


@dataclass(frozen=True)
class Entity:
    name: str  # canonical table name (silver / gold)
    file: str  # extract file name without extension
    d365_entity: str  # D365 data entity (AOT name) the extract follows
    keys: tuple[str, ...]  # canonical business key, used for MERGE
    fields: tuple[Field, ...] = field(default_factory=tuple)

    @property
    def source_columns(self) -> list[str]:
        return [f.source for f in self.fields] + [WATERMARK_COLUMN]

    @property
    def rename(self) -> dict[str, str]:
        return {f.source: f.target for f in self.fields}


F = Field

ENTITIES: dict[str, Entity] = {
    e.name: e
    for e in [
        Entity(
            "plants",
            "OperationalSites",
            "InventOperationalSiteEntity",
            ("plant_id",),
            (
                F("SiteId", "plant_id"),
                F("SiteName", "plant_name"),
                F("PrimaryAddressCountryRegionId", "country"),
                F("CurrencyCode", "currency", False, "Accounting currency of the legal entity the site belongs to"),
            ),
        ),
        Entity(
            "materials",
            "ReleasedProductsV2",
            "EcoResReleasedProductV2Entity",
            ("material_id",),
            (
                F("ItemNumber", "material_id"),
                F("SearchName", "description"),
                F("ItemCategoryCode", "material_type", False, "FG / SFG / RM, from the product category hierarchy"),
                F("ProductGroupId", "product_family"),
                F("BOMUnitSymbol", "base_uom"),
                F("UnitCost", "standard_cost_chf"),
                F("SafetyStockQuantity", "safety_stock_qty", False, "Item coverage settings (minimum on hand)"),
                F("LeadTimeDays", "lead_time_days", False, "Default order settings, purchase lead time"),
            ),
        ),
        Entity(
            "bom",
            "BillOfMaterialsLinesV3",
            "BOMBillOfMaterialsLineV3Entity",
            ("parent_material_id", "component_material_id"),
            (
                F("BOMId", "parent_material_id", True, "Simplified: one active BOM per item, BOMId = parent item"),
                F("ItemNumber", "component_material_id"),
                F("Quantity", "qty_per"),
            ),
        ),
        Entity(
            "work_centers",
            "OperationsResources",
            "OpResOperationsResourceEntity",
            ("work_center_id",),
            (
                F("ResourceId", "work_center_id"),
                F("SiteId", "plant_id", False, "From the resource group the resource is assigned to"),
                F("ResourceName", "work_center_name"),
                F("Capacity", "capacity_hours_per_day"),
            ),
        ),
        Entity(
            "customers",
            "CustomersV3",
            "CustCustomerV3Entity",
            ("customer_id",),
            (
                F("CustomerAccount", "customer_id"),
                F("OrganizationName", "customer_name"),
                F("SalesDistrict", "region"),
                F("SalesSegmentId", "segment"),
            ),
        ),
        Entity(
            "suppliers",
            "VendorsV2",
            "VendVendorV2Entity",
            ("supplier_id",),
            (
                F("VendorAccountNumber", "supplier_id"),
                F("VendorOrganizationName", "supplier_name"),
                F("AddressCountryRegionId", "country"),
            ),
        ),
        Entity(
            "sales_orders",
            "SalesOrderLinesV2",
            "SalesOrderLineV2Entity",
            ("so_id", "so_line"),
            (
                F("SalesOrderNumber", "so_id"),
                F("LineCreationSequenceNumber", "so_line"),
                F(
                    "OrderingCustomerAccountNumber",
                    "customer_id",
                    False,
                    "Header field (SalesOrderHeaderV2Entity), joined into the line extract",
                ),
                F("ItemNumber", "material_id"),
                F("ShippingSiteId", "plant_id"),
                F("OrderCreatedDate", "order_date", False, "Header creation date, joined into the line extract"),
                F("RequestedReceiptDate", "requested_date"),
                F("OrderedSalesQuantity", "order_qty"),
                F("SalesPrice", "net_price_chf", True, "In CHF, the group currency of this model"),
            ),
        ),
        Entity(
            "deliveries",
            "SalesPackingSlipLines",
            "Packing slip journal lines (export view)",
            ("delivery_id",),
            (
                F("PackingSlipLineId", "delivery_id", False),
                F("SalesOrderNumber", "so_id", False),
                F("LineCreationSequenceNumber", "so_line", False),
                F("DeliveryDate", "actual_ship_date", False),
                F("DeliveredQuantity", "shipped_qty", False),
            ),
        ),
        Entity(
            "production_orders",
            "ProductionOrderHeaders",
            "ProdProductionOrderHeaderEntity",
            ("prod_order_id",),
            (
                F("ProductionOrderNumber", "prod_order_id"),
                F("ProductionSiteId", "plant_id"),
                F("ItemNumber", "material_id"),
                F("ResourceId", "work_center_id", False, "Main resource of the route, joined from the route operation"),
                F("ScheduledStartDate", "planned_start"),
                F("ScheduledEndDate", "planned_finish"),
                F("StartedDate", "actual_start"),
                F("EndedDate", "actual_finish"),
                F("ScheduledQuantity", "planned_qty"),
                F("ReportedGoodQuantity", "good_qty", False, "Sum of report-as-finished journal, good quantity"),
                F("ReportedErrorQuantity", "scrap_qty", False, "Sum of report-as-finished journal, error quantity"),
                F("ProductionOrderStatus", "status"),
            ),
        ),
        Entity(
            "inventory_snapshots",
            "InventorySiteOnHandV2",
            "InventorySiteOnHandV2Entity",
            ("snapshot_date", "plant_id", "material_id"),
            (
                F(
                    "SnapshotDate",
                    "snapshot_date",
                    False,
                    "Added by the weekly export, the entity itself is current state",
                ),
                F("InventorySiteId", "plant_id"),
                F("ItemNumber", "material_id"),
                F("OnHandQuantity", "on_hand_qty"),
            ),
        ),
        Entity(
            "purchase_orders",
            "PurchaseOrderLinesV2",
            "PurchPurchaseOrderLineV2Entity",
            ("purchase_order_id",),
            (
                F("PurchaseOrderNumber", "purchase_order_id"),
                F("OrderVendorAccountNumber", "supplier_id", False, "Header field, joined into the line extract"),
                F("ItemNumber", "material_id"),
                F("ReceivingSiteId", "plant_id"),
                F("OrderCreatedDate", "order_date", False, "Header creation date, joined into the line extract"),
                F("ConfirmedDeliveryDate", "promised_date"),
                F("ProductReceiptDate", "received_date", False, "From the product receipt journal"),
                F("OrderedPurchaseQuantity", "order_qty"),
            ),
        ),
    ]
}

SOURCE_TABLES = list(ENTITIES)


def to_source(df, name: str):
    """pandas: canonical columns → extract columns (used by the generator)."""
    e = ENTITIES[name]
    inv = {f.target: f.source for f in e.fields}
    return df.rename(columns=inv)[[f.source for f in e.fields] + ([WATERMARK_COLUMN] if WATERMARK_COLUMN in df else [])]


def to_canonical(df, name: str):
    """Spark: extract columns → canonical columns. Technical columns (``_`` prefix) and the watermark pass through."""
    for src, tgt in ENTITIES[name].rename.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, tgt)
    return df


def mapping_markdown() -> str:
    lines = [
        "| Canonical table | Extract file | D365 entity | Extract field | Canonical column "
        "| Standard entity field | Note |",
        "|---|---|---|---|---|---|---|",
    ]
    for e in ENTITIES.values():
        for f in e.fields:
            lines.append(
                f"| {e.name} | {e.file}.csv | {e.d365_entity} | {f.source} | {f.target} | "
                f"{'yes' if f.standard else 'no'} | {f.note} |"
            )
    return "\n".join(lines)
