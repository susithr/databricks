# Databricks notebook source
# MAGIC %md
# MAGIC # Project 00: set up the ShopStream lakehouse
# MAGIC Run this once. It creates the schema and volume, then writes raw source files for **day 1**.
# MAGIC Later modules call `write_day(n)` to simulate new data arriving.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT 'Databricks DE bootcamp'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{VOLUME} COMMENT 'raw landing zone'")
spark.sql(f"USE {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md ## Generate raw files
# MAGIC The generator is plain Python in `common/datagen.py`. Notebooks in Git folders can import
# MAGIC `.py` files by adding the folder to `sys.path`.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../common"))
from datagen import ShopStreamGenerator

gen = ShopStreamGenerator(seed=42)

def write_day(n: int):
    """Simulate day n of data landing: orders (JSON) + customer CDC (CSV)."""
    print(gen.write_orders_batch(RAW_ORDERS, n))
    print(gen.write_customers_batch(RAW_CUSTOMERS, n))

gen.write_products(RAW_PRODUCTS)
write_day(1)

# COMMAND ----------

display(dbutils.fs.ls(LANDING))

# COMMAND ----------

# MAGIC %md ## Peek at the raw data
# MAGIC `read_files` is a SQL table-valued function that infers the schema of files.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM read_files('/Volumes/workspace/shopstream/landing/orders', format => 'json') LIMIT 20

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT operation, count(*) FROM read_files('/Volumes/workspace/shopstream/landing/customers', format => 'csv', header => true)
# MAGIC GROUP BY operation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reset (only if you want to start the course over)
# MAGIC ```sql
# MAGIC DROP SCHEMA workspace.shopstream CASCADE;
# MAGIC ```
