# Lakeflow Declarative Pipeline: ShopStream Bronze + Silver (Python).
# This file is pipeline source code. Don't run it in a notebook: attach it to a pipeline.
# Legacy equivalent: replace `from pyspark import pipelines as dp` with `import dlt` and
# `dp.` with `dlt.` (materialized_view -> table, create_auto_cdc_flow -> apply_changes).

from pyspark import pipelines as dp
from pyspark.sql import functions as F

LANDING = spark.conf.get("landing_path", "/Volumes/workspace/shopstream/landing")

ORDER_RULES = {
    "has_order_id": "order_id IS NOT NULL",
    "has_customer": "customer_id IS NOT NULL",
    "positive_qty": "quantity > 0",
    "valid_price": "unit_price >= 0",
    "valid_ts": "order_ts IS NOT NULL",
}


# ---------------- BRONZE ----------------
@dp.table(comment="Raw order events from the landing volume (Auto Loader).",
          table_properties={"quality": "bronze"})
def bronze_orders():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.inferColumnTypes", "false")
            .load(f"{LANDING}/orders")
            .select("*", F.col("_metadata.file_path").alias("_source_file"),
                    F.current_timestamp().alias("_ingested_at")))


@dp.table(comment="Raw customer CDC feed.", table_properties={"quality": "bronze"})
def bronze_customers():
    return (spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("header", "true")
            .option("cloudFiles.inferColumnTypes", "true")
            .load(f"{LANDING}/customers"))


@dp.materialized_view(comment="Product catalog snapshot.")
def silver_products():
    return (spark.read.format("json").load(f"{LANDING}/products")
            .withColumn("price", F.col("price").cast("decimal(10,2)")))


# ---------------- SILVER ----------------
@dp.temporary_view(comment="Typed orders (all rows, valid or not).")
def orders_typed():
    ts = lambda c: F.coalesce(F.try_to_timestamp(F.col(c)),
                              F.try_to_timestamp(F.col(c), F.lit("dd/MM/yyyy HH:mm:ss")))
    return (spark.readStream.table("bronze_orders")
            .select(
                F.trim("order_id").alias("order_id"),
                F.expr("try_cast(customer_id AS INT)").alias("customer_id"),
                F.upper("product_id").alias("product_id"),
                F.expr("try_cast(quantity AS INT)").alias("quantity"),
                F.expr("try_cast(unit_price AS DECIMAL(10,2))").alias("unit_price"),
                F.upper("status").alias("status"),
                ts("order_ts").alias("order_ts"),
                F.coalesce(ts("updated_ts"), ts("order_ts")).alias("updated_ts"),
                F.lower("channel").alias("channel"),
                "_source_file"))


@dp.table(comment="Valid order events (append-only).")
@dp.expect_all_or_drop(ORDER_RULES)
def silver_order_events():
    return spark.readStream.table("orders_typed")


@dp.table(comment="Rows that failed at least one rule, kept for investigation.")
def quarantine_orders():
    any_failed = " OR ".join(f"NOT ({r})" for r in ORDER_RULES.values())
    # rules evaluating to NULL count as failures too
    return spark.readStream.table("orders_typed").filter(f"coalesce({any_failed}, true)")


# Current state of each order: AUTO CDC handles duplicates + late status updates (SCD1).
dp.create_streaming_table("silver_orders", comment="One row per order, latest status.")
dp.create_auto_cdc_flow(
    target="silver_orders",
    source="silver_order_events",
    keys=["order_id"],
    sequence_by="updated_ts",
    stored_as_scd_type=1,
)

# Customer dimension with full history (SCD2) straight from the CDC feed.
dp.create_streaming_table("dim_customer", comment="Customer dimension, SCD Type 2.")
dp.create_auto_cdc_flow(
    target="dim_customer",
    source="bronze_customers",
    keys=["customer_id"],
    sequence_by=F.col("operation_ts"),
    apply_as_deletes=F.expr("operation = 'DELETE'"),
    except_column_list=["operation", "operation_ts", "_rescued_data"],
    stored_as_scd_type=2,
)
