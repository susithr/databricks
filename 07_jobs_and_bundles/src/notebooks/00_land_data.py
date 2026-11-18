# Databricks notebook source
# MAGIC %md Simulates the upstream system: each run lands the next day of data.
# MAGIC In a real project this task wouldn't exist. Files would arrive from an app, Kafka or a vendor.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

import os
from shopstream.datagen import ShopStreamGenerator

orders_dir = f"{LANDING}/orders"
existing = [f for f in os.listdir(orders_dir)] if os.path.exists(orders_dir) else []
next_day = len([f for f in existing if f.startswith("orders_")]) + 1

gen = ShopStreamGenerator(seed=42)
gen.write_products(f"{LANDING}/products")
print(gen.write_orders_batch(orders_dir, next_day))
print(gen.write_customers_batch(f"{LANDING}/customers", next_day))
dbutils.jobs.taskValues.set(key="landed_day", value=next_day)   # visible to downstream tasks
