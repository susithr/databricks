# Databricks notebook source
# MAGIC %md
# MAGIC # Simulate new data arriving
# MAGIC `%run ../common/simulate_arrival` and then call `land_day(2)`, `land_day(3)`, …
# MAGIC Each call drops one more day of orders + customer changes into the landing volume,
# MAGIC just like an upstream system would.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../common"))
from datagen import ShopStreamGenerator

_gen = ShopStreamGenerator(seed=42)


def land_day(n: int, orders_per_day: int = 1000):
    _gen.write_products(RAW_PRODUCTS)
    o = _gen.write_orders_batch(RAW_ORDERS, n, orders_per_day)
    c = _gen.write_customers_batch(RAW_CUSTOMERS, n)
    print(f"landed day {n}: {o}, {c}")


def landed_days() -> list[str]:
    try:
        return sorted(f.name for f in dbutils.fs.ls(RAW_ORDERS))
    except Exception:
        return []
