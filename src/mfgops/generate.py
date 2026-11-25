"""Synthetic ERP/MRP data for a fictional precision-valve manufacturer.

Nordhalde Precision AG is invented. It runs three plants (Switzerland,
Malaysia, Romania) and sells vacuum and flow-control components to
semiconductor, display, solar and research customers.

The generator writes ERP-style CSV extracts (the "source system") and
deliberately injects a small share of realistic data-quality defects,
so the data-quality layer has something real to catch.

Usage:
    python -m mfgops.generate --out data/raw --seed 42
"""

from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd

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
                "FERT",
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
                "HALB",
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
                "ROH",
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
    fg = materials.query("material_type == 'FERT'").material_id.tolist()
    sf = materials.query("material_type == 'HALB'").material_id.tolist()
    rm = materials.query("material_type == 'ROH'").material_id.tolist()
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
    fg = materials.query("material_type == 'FERT'")
    n = 9000
    order_date = _random_dates(rng, n)
    mat = rng.choice(fg.material_id, size=n)
    price = fg.set_index("material_id").standard_cost_chf.loc[mat].to_numpy() * rng.uniform(1.35, 1.9, size=n)
    so = pd.DataFrame(
        {
            "so_id": [f"SO{100000 + i // 3}" for i in range(n)],
            "so_line": [10 * (i % 3 + 1) for i in range(n)],
            "customer_id": rng.choice(customers.customer_id, size=n),
            "material_id": mat,
            "plant_id": rng.choice(PLANTS.plant_id, size=n, p=[0.45, 0.35, 0.20]),
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
    makeable = materials.query("material_type in ['FERT', 'HALB']")
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
    status = np.where(actual_finish > END, "REL", "TECO")
    df = pd.DataFrame(
        {
            "prod_order_id": [f"PO{700000 + i}" for i in range(n)],
            "plant_id": plant,
            "material_id": rng.choice(makeable.material_id, size=n),
            "work_center_id": wc,
            "planned_start": planned_start,
            "planned_finish": planned_start + pd.to_timedelta(planned_dur, unit="D"),
            "actual_start": actual_start,
            "actual_finish": actual_finish.where(status == "TECO"),
            "planned_qty": planned_qty,
            "good_qty": np.where(status == "TECO", good_qty, 0),
            "scrap_qty": np.where(status == "TECO", scrap_qty, 0),
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
    rm = materials.query("material_type == 'ROH'")
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
    teco = po.index[po.status == "TECO"]
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


def generate(out_dir: str | Path, seed: int = 42) -> dict[str, int]:
    rng = np.random.default_rng(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)
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
    for name, df in tables.items():
        df.to_csv(out / f"{name}.csv", index=False, date_format="%Y-%m-%d")
    return planted


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--out", default="data/raw")
    p.add_argument("--seed", type=int, default=42)
    a = p.parse_args()
    planted = generate(a.out, a.seed)
    print("Planted defects:", planted)


if __name__ == "__main__":
    main()
