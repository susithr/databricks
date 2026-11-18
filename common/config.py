# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Every notebook runs `%run ../common/config` to get the same names.
# MAGIC Change `CATALOG` if your workspace uses a different default catalog
# MAGIC (Free Edition's default catalog is `workspace`).

# COMMAND ----------

CATALOG = "workspace"
SCHEMA = "shopstream"          # one schema for the whole course
VOLUME = "landing"             # where raw files "arrive"

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"
RAW_ORDERS = f"{LANDING}/orders"        # JSON files, one per batch
RAW_CUSTOMERS = f"{LANDING}/customers"  # CSV CDC files (inserts/updates/deletes)
RAW_PRODUCTS = f"{LANDING}/products"    # one JSON snapshot
CHECKPOINTS = f"{LANDING}/_checkpoints" # streaming checkpoints
SCHEMAS = f"{LANDING}/_schemas"         # Auto Loader schema tracking


def t(name: str) -> str:
    """Fully qualified table name: t('bronze_orders') -> workspace.shopstream.bronze_orders"""
    return f"{CATALOG}.{SCHEMA}.{name}"


try:  # only inside Databricks
    spark.sql(f"USE CATALOG {CATALOG}")
    spark.sql(f"USE SCHEMA {SCHEMA}")
except Exception as e:  # schema not created yet -> run 00_setup first
    print(f"(config) could not USE {CATALOG}.{SCHEMA}: {e}")

print(f"catalog={CATALOG} schema={SCHEMA} landing={LANDING}")
