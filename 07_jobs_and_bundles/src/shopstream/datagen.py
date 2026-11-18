"""Synthetic data generator for the ShopStream e-commerce lakehouse.

Pure Python (no Spark needed) so it runs on Databricks *and* in local tests.
It deliberately produces messy data so the pipelines have something to fix:
  * duplicate order events (same order_id sent twice)
  * nulls / negative quantities / unknown product ids
  * string-typed numbers, mixed timestamp formats
  * late-arriving status updates for existing orders
  * customer CDC feed with INSERT / UPDATE / DELETE operations

Usage inside a notebook:
    import sys; sys.path.append("../common")
    from datagen import ShopStreamGenerator
    gen = ShopStreamGenerator(seed=42)
    gen.write_products(RAW_PRODUCTS)
    gen.write_customers_batch(RAW_CUSTOMERS, batch_no=1)
    gen.write_orders_batch(RAW_ORDERS, batch_no=1)
"""
from __future__ import annotations

import csv
import json
import os
import random
from datetime import datetime, timedelta

COUNTRIES = ["US", "US", "US", "GB", "DE", "IN", "IN", "FR", "BR", "CA"]
CATEGORIES = {
    "electronics": ["Headphones", "Keyboard", "Monitor", "Webcam", "Charger"],
    "home": ["Lamp", "Kettle", "Blender", "Towel Set", "Pillow"],
    "books": ["Spark Guide", "SQL Cookbook", "Data Mesh", "Python 101", "Stats Primer"],
    "sports": ["Yoga Mat", "Dumbbell", "Bottle", "Jump Rope", "Band"],
}
STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED", "RETURNED"]
FIRST = ["Ava", "Liam", "Noah", "Mia", "Arjun", "Priya", "Lucas", "Emma", "Chen", "Sofia", "Omar", "Zara"]
LAST = ["Smith", "Kumar", "Garcia", "Müller", "Rossi", "Silva", "Brown", "Patel", "Wang", "Martin"]


class ShopStreamGenerator:
    def __init__(self, seed: int = 42, n_customers: int = 500, start: datetime | None = None):
        self.seed = seed
        self.rng = random.Random(seed)
        self.n_customers = n_customers
        self.start = start or datetime(2026, 1, 1)
        self.products = self._make_products()

    # ---------- products ----------
    def _make_products(self) -> list[dict]:
        products, pid = [], 1
        for cat, names in CATEGORIES.items():
            for name in names:
                products.append({
                    "product_id": f"P{pid:04d}",
                    "name": name,
                    "category": cat,
                    "price": round(self.rng.uniform(5, 400), 2),
                })
                pid += 1
        return products

    def write_products(self, path: str) -> str:
        os.makedirs(path, exist_ok=True)
        f = os.path.join(path, "products.json")
        with open(f, "w") as fh:
            for p in self.products:
                fh.write(json.dumps(p) + "\n")
        return f

    # ---------- customers (CDC feed) ----------
    def customer_rows(self, batch_no: int) -> list[dict]:
        """Batch 1 = initial load (all INSERTs). Later batches = updates, a few deletes, new customers."""
        rows = []
        ts = self.start + timedelta(days=batch_no - 1)
        if batch_no == 1:
            ids = range(1, self.n_customers + 1)
            ops = ["INSERT"] * self.n_customers
        else:
            changed = random.Random(self.seed * 31 + batch_no).sample(range(1, self.n_customers + 1), k=max(5, self.n_customers // 20))
            new_ids = range(self.n_customers + (batch_no - 2) * 10 + 1, self.n_customers + (batch_no - 1) * 10 + 1)
            ids = list(changed) + list(new_ids)
            ops = ["UPDATE"] * len(changed) + ["INSERT"] * len(new_ids)
            ops[0] = "DELETE"
        for i, (cid, op) in enumerate(zip(ids, ops)):
            ident = random.Random(cid)                 # name/email never change
            r = random.Random(cid * 1000 + batch_no)   # address/tier change per batch
            first, last = ident.choice(FIRST), ident.choice(LAST)
            rows.append({
                "customer_id": cid,
                "first_name": first,
                "last_name": last,
                "email": f"{first.lower()}.{last.lower()}{cid}@example.com",
                "phone": f"+1-555-{r.randint(1000, 9999)}",
                "country": r.choice(COUNTRIES),
                "loyalty_tier": "BRONZE" if op == "INSERT" else r.choice(["SILVER", "GOLD"]),
                "operation": op,
                "operation_ts": (ts + timedelta(seconds=i)).isoformat(sep=" "),
            })
        return rows

    def write_customers_batch(self, path: str, batch_no: int) -> str:
        os.makedirs(path, exist_ok=True)
        rows = self.customer_rows(batch_no)
        f = os.path.join(path, f"customers_{batch_no:03d}.csv")
        with open(f, "w", newline="") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        return f

    # ---------- orders (event stream) ----------
    def _base_orders(self, batch_no: int, n: int) -> list[dict]:
        """Deterministic for (seed, batch_no): any notebook can regenerate any batch."""
        rng = random.Random(self.seed * 7919 + batch_no)
        rows = []
        day = self.start + timedelta(days=batch_no - 1)
        for i in range(n):
            seq = (batch_no - 1) * n + i + 1
            p = rng.choice(self.products)
            ts = day + timedelta(seconds=rng.randint(0, 86399))
            row = {
                "order_id": f"O{seq:08d}",
                "customer_id": rng.randint(1, self.n_customers),
                "product_id": p["product_id"],
                "quantity": rng.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0],
                "unit_price": p["price"],
                "status": "PLACED",
                "order_ts": ts.isoformat(),
                "updated_ts": ts.isoformat(),
                "channel": rng.choice(["web", "web", "app", "app", "store"]),
            }
            # --- inject mess (about 5% of rows) ---
            roll = rng.random()
            if roll < 0.01:
                row["quantity"] = -row["quantity"]          # invalid quantity
            elif roll < 0.02:
                row["customer_id"] = None                   # missing FK
            elif roll < 0.03:
                row["product_id"] = "P9999"                 # unknown product
            elif roll < 0.04:
                row["unit_price"] = str(row["unit_price"])  # wrong type
            elif roll < 0.05:
                row["order_ts"] = ts.strftime("%d/%m/%Y %H:%M:%S")  # wrong format
            rows.append(row)
            if rng.random() < 0.02:                         # exact duplicate event
                rows.append(dict(row))
        return rows

    def order_rows(self, batch_no: int, n: int = 1000) -> list[dict]:
        rows = self._base_orders(batch_no, n)
        if batch_no > 1:  # late status updates for yesterday's orders
            rng = random.Random(self.seed * 104729 + batch_no)
            day = self.start + timedelta(days=batch_no - 1)
            prev = [r for r in self._base_orders(batch_no - 1, n) if isinstance(r["quantity"], int)]
            for old in rng.sample(prev, k=min(len(prev), n // 5)):
                upd = dict(old)
                upd["status"] = rng.choice(STATUSES[1:])
                upd["updated_ts"] = (day + timedelta(seconds=rng.randint(0, 86399))).isoformat()
                rows.append(upd)
        return rows

    def write_orders_batch(self, path: str, batch_no: int, n: int = 1000) -> str:
        os.makedirs(path, exist_ok=True)
        f = os.path.join(path, f"orders_{batch_no:03d}.json")
        with open(f, "w") as fh:
            for r in self.order_rows(batch_no, n):
                fh.write(json.dumps(r) + "\n")
        return f
