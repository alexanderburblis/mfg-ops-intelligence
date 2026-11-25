"""Synthetic ERP/MRP data for a fictional precision-valve manufacturer.

Nordhalde Precision AG is invented. It runs three plants (Switzerland,
Malaysia, Romania) and sells vacuum and flow-control components to
semiconductor, display, solar and research customers.

The generator writes ERP-style CSV extracts (the "source system") and
deliberately injects a small share of realistic data-quality defects,
so the data-quality layer has something real to catch.

The extracts follow Dynamics 365 data entity names (see ``source_map``).
A second, smaller change extract simulates the next scheduled export, for
the incremental load.

Usage:
    python -m mfgops.generate --out data/raw --seed 42
    python -m mfgops.generate --out data/increments/2026-11-15 --increment
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

from mfgops.source_map import ENTITIES, WATERMARK_COLUMN, to_source

START = pd.Timestamp("2025-01-01")
END = pd.Timestamp("2026-10-31")

PLANTS = pd.DataFrame(
    [
        ("CH01", "Rheintal Assembly", "CH", "CHF"),
        ("MY01", "Penang Machining", "MY", "MYR"),
        ("RO01", "Cluj Components", "RO", "RON"),
    ],
    columns=["plant_id", "plant_name", "country", "currency"],
)

PRODUCT_FAMILIES = ["Gate Valves", "Pendulum Valves", "Angle Valves", "Transfer Valves", "Bellows"]
SEGMENTS = ["Semiconductor", "Display", "Solar", "Research", "Industrial"]
REGIONS = ["EMEA", "Americas", "Asia-Pacific", "Greater China"]


def _materials(rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    # Finished goods
    for i in range(1, 41):
        fam = PRODUCT_FAMILIES[(i - 1) % len(PRODUCT_FAMILIES)]
        rows.append(
            (
                f"FG-{i:04d}",
                f"{fam[:-1]} Series {100 + i}",
                "FG",
                fam,
                "PC",
                round(float(rng.uniform(1800, 14000)), 2),
                int(rng.integers(5, 25)),
                int(rng.integers(10, 30)),
            )
        )
    # Semi-finished goods
    for i in range(1, 31):
        rows.append(
            (
                f"SF-{i:04d}",
                f"Sub-assembly {i}",
                "SFG",
                "Sub-assemblies",
                "PC",
                round(float(rng.uniform(250, 2500)), 2),
                int(rng.integers(10, 60)),
                int(rng.integers(5, 15)),
            )
        )
    # Raw materials
    for i in range(1, 61):
        kind = ["Stainless bar", "Aluminium block", "Elastomer seal", "Bearing", "Actuator", "Sensor"][i % 6]
        rows.append(
            (
                f"RM-{i:04d}",
                f"{kind} {i}",
                "RM",
                "Raw materials",
                "PC",
                round(float(rng.uniform(5, 400)), 2),
                int(rng.integers(50, 400)),
                int(rng.integers(7, 60)),
            )
        )
    return pd.DataFrame(
        rows,
        columns=[
            "material_id",
            "description",
            "material_type",
            "product_family",
            "base_uom",
            "standard_cost_chf",
            "safety_stock_qty",
            "lead_time_days",
        ],
    )


def _bom(rng: np.random.Generator, materials: pd.DataFrame) -> pd.DataFrame:
    fg = materials.query("material_type == 'FG'").material_id.tolist()
    sf = materials.query("material_type == 'SFG'").material_id.tolist()
    rm = materials.query("material_type == 'RM'").material_id.tolist()
    rows = []
    for parent in fg:
        for comp in rng.choice(sf, size=3, replace=False):
            rows.append((parent, comp, int(rng.integers(1, 4))))
        for comp in rng.choice(rm, size=2, replace=False):
            rows.append((parent, comp, int(rng.integers(1, 8))))
    for parent in sf:
        for comp in rng.choice(rm, size=3, replace=False):
            rows.append((parent, comp, int(rng.integers(1, 6))))
    return pd.DataFrame(rows, columns=["parent_material_id", "component_material_id", "qty_per"])


def _work_centers() -> pd.DataFrame:
    rows = []
    for plant in PLANTS.plant_id:
        for j, name in enumerate(["CNC Machining", "Cleanroom Assembly", "Leak Testing", "Final Inspection"], 1):
            rows.append((f"{plant}-WC{j}", plant, name, [16, 20, 12, 10][j - 1]))
    return pd.DataFrame(rows, columns=["work_center_id", "plant_id", "work_center_name", "capacity_hours_per_day"])


def _customers(rng: np.random.Generator) -> pd.DataFrame:
    rows = []
    for i in range(1, 121):
        rows.append((f"C{i:05d}", f"Customer {i:03d}", str(rng.choice(REGIONS)), str(rng.choice(SEGMENTS))))
    return pd.DataFrame(rows, columns=["customer_id", "customer_name", "region", "segment"])


def _suppliers(rng: np.random.Generator) -> pd.DataFrame:
    rows = [
        (f"S{i:04d}", f"Supplier {i:02d}", str(rng.choice(["CH", "DE", "MY", "RO", "JP", "US"]))) for i in range(1, 26)
    ]
    return pd.DataFrame(rows, columns=["supplier_id", "supplier_name", "country"])


def _random_dates(rng: np.random.Generator, n: int, start=START, end=END) -> pd.Series:
    span = (end - start).days
    return pd.to_datetime(start) + pd.to_timedelta(rng.integers(0, span, size=n), unit="D")


def _sales(rng, materials, customers):
    fg = materials.query("material_type == 'FG'")
    n = 9000
    # Header values (customer, order date, shipping site) are the same on every line of an order.
    order_date = pd.Series(_random_dates(rng, n // 3)).repeat(3).reset_index(drop=True)
    mat = rng.choice(fg.material_id, size=n)
    price = fg.set_index("material_id").standard_cost_chf.loc[mat].to_numpy() * rng.uniform(1.35, 1.9, size=n)
    so = pd.DataFrame(
        {
            "so_id": [f"SO{100000 + i // 3}" for i in range(n)],
            "so_line": [10 * (i % 3 + 1) for i in range(n)],
            "customer_id": rng.choice(customers.customer_id, size=n // 3).repeat(3),
            "material_id": mat,
            "plant_id": rng.choice(PLANTS.plant_id, size=n // 3, p=[0.45, 0.35, 0.20]).repeat(3),
            "order_date": order_date,
            "requested_date": order_date + pd.to_timedelta(rng.integers(14, 70, size=n), unit="D"),
            "order_qty": rng.integers(1, 30, size=n),
            "net_price_chf": np.round(price, 2),
        }
    )
    # Deliveries: most shipped, a share late, a share partial, recent ones open.
    delay = rng.choice([-5, -2, 0, 0, 0, 1, 3, 7, 14], size=n, p=[0.1, 0.15, 0.3, 0.15, 0.1, 0.07, 0.06, 0.04, 0.03])
    ship_date = so.requested_date + pd.to_timedelta(delay, unit="D")
    shipped_qty = np.where(rng.random(n) < 0.06, np.maximum(so.order_qty - rng.integers(1, 3, size=n), 0), so.order_qty)
    deliv = pd.DataFrame(
        {
            "delivery_id": [f"DL{500000 + i}" for i in range(n)],
            "so_id": so.so_id,
            "so_line": so.so_line,
            "actual_ship_date": ship_date,
            "shipped_qty": shipped_qty,
        }
    )
    deliv = deliv[deliv.actual_ship_date <= END].reset_index(drop=True)
    return so, deliv


def _production(rng, materials, work_centers):
    makeable = materials.query("material_type in ['FG', 'SFG']")
    n = 7000
    plant = rng.choice(PLANTS.plant_id, size=n, p=[0.45, 0.35, 0.20])
    wc = [f"{p}-WC{int(rng.integers(1, 5))}" for p in plant]
    planned_start = _random_dates(rng, n)
    planned_dur = rng.integers(2, 12, size=n)
    start_slip = rng.choice([0, 0, 0, 0, 0, 0, 1, 2, 4], size=n)
    run_slip = rng.choice([-2, -1, -1, 0, 0, 0, 0, 1, 3], size=n)
    planned_qty = rng.integers(5, 120, size=n)
    scrap_rate = np.clip(rng.normal(0.025, 0.02, size=n), 0, 0.2)
    plant_effect = np.select([plant == "MY01", plant == "RO01"], [0.01, 0.015], 0.0)
    scrap_qty = np.round(planned_qty * (scrap_rate + plant_effect)).astype(int)
    good_qty = np.maximum(planned_qty - scrap_qty - rng.choice([0, 0, 0, 1, 2], size=n), 0)
    actual_start = planned_start + pd.to_timedelta(start_slip, unit="D")
    actual_finish = actual_start + pd.to_timedelta(planned_dur + run_slip, unit="D")
    status = np.where(actual_finish > END, "Started", "Ended")
    df = pd.DataFrame(
        {
            "prod_order_id": [f"PO{700000 + i}" for i in range(n)],
            "plant_id": plant,
            "material_id": rng.choice(makeable.material_id, size=n),
            "work_center_id": wc,
            "planned_start": planned_start,
            "planned_finish": planned_start + pd.to_timedelta(planned_dur, unit="D"),
            "actual_start": actual_start,
            "actual_finish": actual_finish.where(status == "Ended"),
            "planned_qty": planned_qty,
            "good_qty": np.where(status == "Ended", good_qty, 0),
            "scrap_qty": np.where(status == "Ended", scrap_qty, 0),
            "status": status,
        }
    )
    return df


def _inventory(rng, materials):
    weeks = pd.date_range(START, END, freq="W-FRI")
    rows = []
    for plant in PLANTS.plant_id:
        for _, m in materials.iterrows():
            level = m.safety_stock_qty * rng.uniform(1.0, 3.0)
            for w in weeks:
                level = max(level + rng.normal(0, m.safety_stock_qty * 0.25), 0)
                rows.append((w, plant, m.material_id, int(round(level))))
    return pd.DataFrame(rows, columns=["snapshot_date", "plant_id", "material_id", "on_hand_qty"])


def _purchasing(rng, materials, suppliers):
    rm = materials.query("material_type == 'RM'")
    n = 6000
    order_date = _random_dates(rng, n)
    lead = rm.set_index("material_id").lead_time_days
    mat = rng.choice(rm.material_id, size=n)
    promised = order_date + pd.to_timedelta(lead.loc[mat].to_numpy(), unit="D")
    slip = rng.choice([-3, 0, 0, 0, 2, 5, 10, 20], size=n, p=[0.1, 0.35, 0.15, 0.1, 0.12, 0.08, 0.06, 0.04])
    received = promised + pd.to_timedelta(slip, unit="D")
    return pd.DataFrame(
        {
            "purchase_order_id": [f"PU{900000 + i}" for i in range(n)],
            "supplier_id": rng.choice(suppliers.supplier_id, size=n),
            "material_id": mat,
            "plant_id": rng.choice(PLANTS.plant_id, size=n),
            "order_date": order_date,
            "promised_date": promised,
            "received_date": received.where(received <= END),
            "order_qty": rng.integers(20, 800, size=n),
        }
    )


def _inject_defects(rng, t: dict[str, pd.DataFrame]) -> dict[str, int]:
    """Inject known defects and return how many of each were planted."""
    planted = {}
    so = t["sales_orders"]
    idx = rng.choice(so.index, size=40, replace=False)
    so.loc[idx[:15], "customer_id"] = "C99999"  # unknown customer
    so.loc[idx[15:25], "order_qty"] = -so.loc[idx[15:25], "order_qty"]  # negative quantity
    so.loc[idx[25:40], "requested_date"] = pd.NaT  # missing requested date
    planted.update(so_unknown_customer=15, so_negative_qty=10, so_missing_requested_date=15)

    po = t["production_orders"]
    teco = po.index[po.status == "Ended"]
    idx = rng.choice(teco, size=30, replace=False)
    po.loc[idx[:12], "actual_finish"] = po.loc[idx[:12], "actual_start"] - pd.Timedelta(days=2)  # finish before start
    po.loc[idx[12:30], "scrap_qty"] = po.loc[idx[12:30], "planned_qty"] * 3  # implausible scrap
    dupes = po.sample(8, random_state=int(rng.integers(0, 10_000)))
    t["production_orders"] = pd.concat([po, dupes], ignore_index=True)  # duplicate order ids
    planted.update(prod_finish_before_start=12, prod_implausible_yield=18, prod_duplicate_id=8)

    inv = t["inventory_snapshots"]
    idx = rng.choice(inv.index, size=25, replace=False)
    inv.loc[idx, "on_hand_qty"] = -inv.loc[idx, "on_hand_qty"] - 1  # negative stock
    planted.update(inv_negative_stock=25)

    dl = t["deliveries"]
    idx = rng.choice(dl.index, size=10, replace=False)
    dl.loc[idx, "so_id"] = "SO000000"  # delivery without order
    planted.update(dlv_orphan=10)
    return planted


INITIAL_LOAD_TS = "2026-10-31T23:00:00"
INCREMENT_TS = "2026-11-15T02:00:00"
INCREMENT_END = pd.Timestamp("2026-11-14")


def build_base(seed: int = 42) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    """All tables of the initial full extract, canonical column names, defects included."""
    rng = np.random.default_rng(seed)
    materials = _materials(rng)
    customers = _customers(rng)
    suppliers = _suppliers(rng)
    work_centers = _work_centers()
    so, deliv = _sales(rng, materials, customers)
    tables = {
        "plants": PLANTS.copy(),
        "materials": materials,
        "bom": _bom(rng, materials),
        "work_centers": work_centers,
        "customers": customers,
        "suppliers": suppliers,
        "sales_orders": so,
        "deliveries": deliv,
        "production_orders": _production(rng, materials, work_centers),
        "inventory_snapshots": _inventory(rng, materials),
        "purchase_orders": _purchasing(rng, materials, suppliers),
    }
    planted = _inject_defects(rng, tables)
    return tables, planted


def write_extracts(tables: dict[str, pd.DataFrame], out_dir: str | Path, modified: str) -> None:
    """Write canonical tables as D365-shaped extract files, stamped with the change-tracking timestamp."""
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
    for name, df in tables.items():
        df = df.copy()
        df[WATERMARK_COLUMN] = modified
        to_source(df, name).to_csv(out / f"{ENTITIES[name].file}.csv", index=False, date_format="%Y-%m-%d")


def generate(out_dir: str | Path, seed: int = 42) -> dict[str, int]:
    """Initial full extract. Returns the number of planted defects per kind."""
    tables, planted = build_base(seed)
    write_extracts(tables, out_dir, INITIAL_LOAD_TS)
    return planted


def build_increment(seed: int = 42) -> tuple[dict[str, pd.DataFrame], dict[str, int]]:
    """Changes between the initial extract (31 Oct 2026) and the next run (15 Nov 2026).

    New and changed rows only, the way a change-tracking export delivers them:
    new customers and customers that moved segment (slowly changing dimension),
    new order lines, shipments for open lines, production orders that started
    or ended, two weekly stock snapshots, goods receipts for open purchase orders.
    Two new order lines carry a negative quantity, so quality rules run on
    the delta as well.
    """
    base, _ = build_base(seed)
    rng = np.random.default_rng(seed + 1000)
    window_start = END + pd.Timedelta(days=1)
    planted: dict[str, int] = {}

    # Customers: 6 move to another segment, 3 are new.
    cust = base["customers"]
    movers = cust.sample(6, random_state=seed).copy()
    movers["segment"] = [SEGMENTS[(SEGMENTS.index(s) + 1) % len(SEGMENTS)] for s in movers.segment]
    new_c = pd.DataFrame(
        [
            (f"C{i:05d}", f"Customer {i:03d}", str(rng.choice(REGIONS)), str(rng.choice(SEGMENTS)))
            for i in (121, 122, 123)
        ],
        columns=cust.columns,
    )
    customers = pd.concat([movers, new_c], ignore_index=True)

    # New sales order lines.
    fg = base["materials"].query("material_type == 'FG'")
    n = 300
    order_date = pd.Series(_random_dates(rng, n // 3, window_start, INCREMENT_END)).repeat(3).reset_index(drop=True)
    mat = rng.choice(fg.material_id, size=n)
    price = fg.set_index("material_id").standard_cost_chf.loc[mat].to_numpy() * rng.uniform(1.35, 1.9, size=n)
    first = int(base["sales_orders"].so_id.str[2:].astype(int).max()) + 1
    so = pd.DataFrame(
        {
            "so_id": [f"SO{first + i // 3}" for i in range(n)],
            "so_line": [10 * (i % 3 + 1) for i in range(n)],
            "customer_id": rng.choice(pd.concat([cust.customer_id, new_c.customer_id]), size=n // 3).repeat(3),
            "material_id": mat,
            "plant_id": rng.choice(PLANTS.plant_id, size=n // 3, p=[0.45, 0.35, 0.20]).repeat(3),
            "order_date": order_date,
            "requested_date": order_date + pd.to_timedelta(rng.integers(14, 70, size=n), unit="D"),
            "order_qty": rng.integers(1, 30, size=n),
            "net_price_chf": np.round(price, 2),
        }
    )
    so.loc[so.sample(2, random_state=seed).index, "order_qty"] = -3
    planted["inc_so_negative_qty"] = 2

    # Shipments for open lines that fell due in the window.
    base_so, base_dl = base["sales_orders"], base["deliveries"]
    shipped = base_dl[["so_id", "so_line"]].drop_duplicates()
    open_lines = base_so.merge(shipped, on=["so_id", "so_line"], how="left", indicator=True)
    open_lines = open_lines[
        (open_lines["_merge"] == "left_only")
        & open_lines.requested_date.notna()
        & (open_lines.requested_date <= INCREMENT_END + pd.Timedelta(days=5))
        & (open_lines.order_qty > 0)
    ]
    ship = open_lines.requested_date + pd.to_timedelta(rng.choice([-3, -1, 0, 0, 2, 5], size=len(open_lines)), unit="D")
    ship = ship.clip(upper=INCREMENT_END).clip(lower=window_start)
    first_dl = int(base_dl.delivery_id.str[2:].astype(int).max()) + 1
    deliveries = pd.DataFrame(
        {
            "delivery_id": [f"DL{first_dl + i}" for i in range(len(open_lines))],
            "so_id": open_lines.so_id.to_numpy(),
            "so_line": open_lines.so_line.to_numpy(),
            "actual_ship_date": ship.to_numpy(),
            "shipped_qty": open_lines.order_qty.to_numpy(),
        }
    )

    # Production: started orders that are now due get ended, plus new started orders.
    po = base["production_orders"].drop_duplicates("prod_order_id")
    due = po[(po.status == "Started") & (po.planned_finish <= INCREMENT_END)].copy()
    due["actual_finish"] = (
        due.planned_finish + pd.to_timedelta(rng.choice([0, 0, 1, 2], size=len(due)), unit="D")
    ).clip(upper=INCREMENT_END)
    due["actual_finish"] = due[["actual_finish", "actual_start"]].max(axis=1)
    scrap = np.round(due.planned_qty * np.clip(rng.normal(0.03, 0.015, size=len(due)), 0, 0.15)).astype(int)
    due["scrap_qty"] = scrap
    due["good_qty"] = due.planned_qty - scrap
    due["status"] = "Ended"
    n_new = 120
    plant = rng.choice(PLANTS.plant_id, size=n_new, p=[0.45, 0.35, 0.20])
    start = _random_dates(rng, n_new, window_start, INCREMENT_END)
    dur = rng.integers(2, 12, size=n_new)
    first_po = int(po.prod_order_id.str[2:].astype(int).max()) + 1
    makeable = base["materials"].query("material_type in ['FG', 'SFG']")
    new_po = pd.DataFrame(
        {
            "prod_order_id": [f"PO{first_po + i}" for i in range(n_new)],
            "plant_id": plant,
            "material_id": rng.choice(makeable.material_id, size=n_new),
            "work_center_id": [f"{p}-WC{int(rng.integers(1, 5))}" for p in plant],
            "planned_start": start,
            "planned_finish": start + pd.to_timedelta(dur, unit="D"),
            "actual_start": start,
            "actual_finish": pd.NaT,
            "planned_qty": rng.integers(5, 120, size=n_new),
            "good_qty": 0,
            "scrap_qty": 0,
            "status": "Started",
        }
    )
    production = pd.concat([due, new_po], ignore_index=True)

    # Two weekly stock snapshots continuing from the last known level.
    inv = base["inventory_snapshots"]
    last = inv[inv.snapshot_date == inv.snapshot_date.max()].copy()
    last["on_hand_qty"] = last.on_hand_qty.clip(lower=0)
    safety = base["materials"].set_index("material_id").safety_stock_qty
    snaps = []
    for d in pd.date_range(window_start, INCREMENT_END, freq="W-FRI"):
        step = rng.normal(0, safety.loc[last.material_id].to_numpy() * 0.25)
        last = last.assign(snapshot_date=d, on_hand_qty=np.maximum(last.on_hand_qty + step, 0).round().astype(int))
        snaps.append(last.copy())
    inventory = pd.concat(snaps, ignore_index=True)

    # Goods receipts for open purchase orders now due, plus new purchase orders.
    pu = base["purchase_orders"]
    due_pu = pu[pu.received_date.isna() & (pu.promised_date <= INCREMENT_END)].copy()
    due_pu["received_date"] = (
        due_pu.promised_date + pd.to_timedelta(rng.choice([-2, 0, 0, 3, 8], size=len(due_pu)), unit="D")
    ).clip(lower=window_start, upper=INCREMENT_END)
    rm = base["materials"].query("material_type == 'RM'")
    n_pu = 200
    od = _random_dates(rng, n_pu, window_start, INCREMENT_END)
    m = rng.choice(rm.material_id, size=n_pu)
    first_pu = int(pu.purchase_order_id.str[2:].astype(int).max()) + 1
    new_pu = pd.DataFrame(
        {
            "purchase_order_id": [f"PU{first_pu + i}" for i in range(n_pu)],
            "supplier_id": rng.choice(base["suppliers"].supplier_id, size=n_pu),
            "material_id": m,
            "plant_id": rng.choice(PLANTS.plant_id, size=n_pu),
            "order_date": od,
            "promised_date": od
            + pd.to_timedelta(rm.set_index("material_id").lead_time_days.loc[m].to_numpy(), unit="D"),
            "received_date": pd.NaT,
            "order_qty": rng.integers(20, 800, size=n_pu),
        }
    )
    purchasing = pd.concat([due_pu, new_pu], ignore_index=True)

    changes = {
        "customers": customers,
        "sales_orders": so,
        "deliveries": deliveries,
        "production_orders": production,
        "inventory_snapshots": inventory,
        "purchase_orders": purchasing,
    }
    return changes, planted


def generate_increment(out_dir: str | Path, seed: int = 42) -> dict[str, int]:
    """Write the 15 Nov 2026 change extract (only the entities that changed)."""
    changes, planted = build_increment(seed)
    write_extracts(changes, out_dir, INCREMENT_TS)
    return {**{f"rows_{k}": len(v) for k, v in changes.items()}, **planted}


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="data/raw")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--increment", action="store_true", help="write the 15 Nov 2026 change extract instead")
    a = p.parse_args()
    if a.increment:
        print("Increment:", generate_increment(a.out, a.seed))
    else:
        print("Planted defects:", generate(a.out, a.seed))


if __name__ == "__main__":
    main()
