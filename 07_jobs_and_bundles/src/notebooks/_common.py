# Databricks notebook source
# Shared setup for every task: read job parameters, compute paths, make `shopstream` importable.
import os, sys

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "shopstream_dev")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.landing")

LANDING = f"/Volumes/{CATALOG}/{SCHEMA}/landing"
CHECKPOINTS = f"{LANDING}/_checkpoints"
SCHEMAS = f"{LANDING}/_schemas"

def t(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"

sys.path.append(os.path.abspath(".."))   # src/ -> import shopstream.*
print(f"catalog={CATALOG} schema={SCHEMA}")
