# Databricks notebook source
# MAGIC %md
# MAGIC # Project 04: incremental ingestion with Auto Loader, Structured Streaming and SCD2
# MAGIC All streams use `trigger(availableNow=True)`: process what's new, then stop. Rerun the notebook after landing new days.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/simulate_arrival

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../common"))
from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable
from transforms import clean_orders, split_valid, latest_per_key, enrich_orders

dbutils.widgets.text("land_day", "0", "Land this day first (0 = none)")
if int(dbutils.widgets.get("land_day")):
    land_day(int(dbutils.widgets.get("land_day")))
print(landed_days())

def cp(name):  # one checkpoint folder per stream
    return f"{CHECKPOINTS}/m04/{name}"

# COMMAND ----------

# MAGIC %md ## 1. Bronze: Auto Loader
# MAGIC Orders as JSON with everything kept as strings; anything weird goes into `_rescued_data`.

# COMMAND ----------

def autoload(path, fmt, name, **opts):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{SCHEMAS}/m04/{name}")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for k, v in opts.items():
        reader = reader.option(k, v)
    return (reader.load(path)
            .withColumn("_source_file", F.col("_metadata.file_path"))
            .withColumn("_ingested_at", F.current_timestamp()))

q = (autoload(RAW_ORDERS, "json", "orders", **{"cloudFiles.inferColumnTypes": "false"})
     .writeStream.option("checkpointLocation", cp("bronze_orders_al"))
     .option("mergeSchema", "true")
     .trigger(availableNow=True)
     .toTable(t("bronze_orders_al")))
q.awaitTermination()
print("orders rows in last batch:", q.lastProgress["numInputRows"] if q.lastProgress else 0)

# COMMAND ----------

q = (autoload(RAW_CUSTOMERS, "csv", "customers", header="true",
              **{"cloudFiles.inferColumnTypes": "true"})
     .writeStream.option("checkpointLocation", cp("bronze_customers"))
     .option("mergeSchema", "true")
     .trigger(availableNow=True)
     .toTable(t("bronze_customers")))
q.awaitTermination()
display(spark.table(t("bronze_customers")).groupBy("_source_file", "operation").count())

# COMMAND ----------

# MAGIC %md ## 2. Silver orders: stream + `foreachBatch` MERGE
# MAGIC Reading a Delta table as a stream only picks up **new** rows since the last checkpoint.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('silver_orders_inc')} (
  order_id STRING NOT NULL, customer_id INT, product_id STRING, product_name STRING, category STRING,
  quantity INT, unit_price DECIMAL(10,2), amount DECIMAL(12,2), status STRING, channel STRING,
  order_ts TIMESTAMP, updated_ts TIMESTAMP, order_date DATE, _ingested_at TIMESTAMP)
""")
spark.sql(f"CREATE TABLE IF NOT EXISTS {t('quarantine_orders_inc')} (order_id STRING, dq_errors ARRAY<STRING>, _source_file STRING, _ingested_at TIMESTAMP)")

def upsert_orders(batch_df, batch_id):
    products = spark.table(t("silver_products"))   # created in Module 03
    valid, bad = split_valid(clean_orders(batch_df))
    bad.select("order_id", "dq_errors", "_source_file", "_ingested_at") \
       .write.mode("append").saveAsTable(t("quarantine_orders_inc"))
    src = enrich_orders(latest_per_key(valid, "order_id", "updated_ts"), products)
    cols = spark.table(t("silver_orders_inc")).columns
    (DeltaTable.forName(spark, t("silver_orders_inc")).alias("t")
        .merge(src.select(*cols).alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.updated_ts > t.updated_ts")
        .whenNotMatchedInsertAll()
        .execute())

q = (spark.readStream.table(t("bronze_orders_al"))
     .writeStream.foreachBatch(upsert_orders)
     .option("checkpointLocation", cp("silver_orders_inc"))
     .trigger(availableNow=True).start())
q.awaitTermination()
print(spark.table(t("silver_orders_inc")).count(), "silver orders")

# COMMAND ----------

# MAGIC %md ## 3. `dim_customer` as SCD Type 2
# MAGIC Columns: `valid_from`, `valid_to` (NULL = current), `is_current`.
# MAGIC The union trick: rows with `merge_key = NULL` never match, so they INSERT the new version; rows with the real key close the old version.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('dim_customer')} (
  customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
  customer_id INT, first_name STRING, last_name STRING, email STRING, phone STRING,
  country STRING, loyalty_tier STRING,
  valid_from TIMESTAMP, valid_to TIMESTAMP, is_current BOOLEAN)
""")

TRACKED = ["first_name", "last_name", "email", "phone", "country", "loyalty_tier"]

def scd2_customers(batch_df, batch_id):
    dim = DeltaTable.forName(spark, t("dim_customer"))
    dim_df = spark.table(t("dim_customer"))
    current = dim_df.filter("is_current")

    # Idempotency guard: ignore changes not newer than what we've already applied for that customer.
    # Without this, a replayed batch (job rerun, checkpoint reset) would resurrect deleted customers.
    last_applied = dim_df.groupBy("customer_id").agg(
        F.max(F.greatest("valid_from", F.coalesce("valid_to", "valid_from"))).alias("_last_applied"))
    changes = (latest_per_key(batch_df, "customer_id", "operation_ts")
               .join(last_applied, "customer_id", "left")
               .filter(F.col("_last_applied").isNull() | (F.col("operation_ts") > F.col("_last_applied")))
               .drop("_last_applied"))

    # rows whose tracked attributes actually changed (or are new) need a new version
    changed = (changes.alias("s").join(current.alias("c"), "customer_id", "left")
               .filter(F.col("s.operation") != "DELETE")
               .filter(F.col("c.customer_id").isNull() |
                       ~F.expr(" AND ".join(f"s.{c} <=> c.{c}" for c in TRACKED)))
               .select("s.*"))

    staged = (changes.withColumn("merge_key", F.col("customer_id"))               # closes old rows
              .unionByName(changed.withColumn("merge_key", F.lit(None).cast("int"))))  # inserts new rows

    (dim.alias("t").merge(staged.alias("s"), "t.customer_id = s.merge_key AND t.is_current")
        .whenMatchedUpdate(
            condition="s.operation = 'DELETE' OR NOT (" + " AND ".join(f"t.{c} <=> s.{c}" for c in TRACKED) + ")",
            set={"valid_to": "s.operation_ts", "is_current": "false"})
        .whenNotMatchedInsert(
            condition="s.merge_key IS NULL",   # only the 'new version' copy inserts
            values={"customer_id": "s.customer_id", **{c: f"s.{c}" for c in TRACKED},
                    "valid_from": "s.operation_ts", "valid_to": "null", "is_current": "true"})
        .execute())

q = (spark.readStream.table(t("bronze_customers"))
     .withColumn("operation_ts", F.to_timestamp("operation_ts"))
     .writeStream.foreachBatch(scd2_customers)
     .option("checkpointLocation", cp("dim_customer"))
     .trigger(availableNow=True).start())
q.awaitTermination()

# COMMAND ----------

# MAGIC %sql
# MAGIC -- customers with history
# MAGIC SELECT customer_id, country, loyalty_tier, valid_from, valid_to, is_current
# MAGIC FROM dim_customer
# MAGIC WHERE customer_id IN (SELECT customer_id FROM dim_customer GROUP BY 1 HAVING count(*) > 1)
# MAGIC ORDER BY customer_id, valid_from
# MAGIC LIMIT 30

# COMMAND ----------

# MAGIC %md ## 4. Streaming aggregation with a watermark
# MAGIC Orders per hourly window. The watermark tells Spark how late data may arrive before a window is final.

# COMMAND ----------

# silver_orders_inc is updated by MERGE. A plain readStream would fail on non-append commits,
# so skipChangeCommits tells the reader to ignore them (use CDF if you need the updates themselves)
hourly = (spark.readStream.option("skipChangeCommits", "true").table(t("silver_orders_inc"))
          .withWatermark("order_ts", "2 hours")
          .groupBy(F.window("order_ts", "1 hour").alias("w"), "channel")
          .agg(F.count("*").alias("orders"))
          .select(F.col("w.start").alias("window_start"), "channel", "orders"))
q = (hourly.writeStream.outputMode("append")
     .option("checkpointLocation", cp("gold_hourly_orders"))
     .trigger(availableNow=True).toTable(t("gold_hourly_orders")))
q.awaitTermination()
display(spark.table(t("gold_hourly_orders")).orderBy("window_start"))
# Note: in append mode a window is written only once the watermark passes it,
# so the newest hours show up on the *next* run, after later data arrives.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Now: land day 3 (set the widget to 3) and rerun everything.
# MAGIC Check `q.recentProgress` / `numInputRows`: only new files are read. Then look at `dim_customer` again.
# MAGIC
# MAGIC ## Your turn
# MAGIC 1. **Schema evolution:** write a JSON file into `RAW_ORDERS` with an extra `coupon_code` field. Run the Bronze cell: it fails once with an `UnknownFieldException`. Run it again and the column is there.
# MAGIC 2. **Point-in-time query:** customer 82's country as of `2026-01-01 12:00` vs now.
# MAGIC 3. Run the orders bronze stream with `trigger(processingTime="10 seconds")`, call `land_day(4)` from another cell, watch it get picked up, then `q.stop()`.
# MAGIC
# MAGIC ---
# MAGIC ## SOLUTION

# COMMAND ----------

import json
with open(f"{RAW_ORDERS}/orders_coupon_test.json", "w") as fh:
    fh.write(json.dumps({"order_id": "OCOUPON1", "customer_id": 1, "product_id": "P0001", "quantity": 1,
                         "unit_price": 9.99, "status": "PLACED", "order_ts": "2026-01-05T10:00:00",
                         "channel": "web", "coupon_code": "WELCOME10"}) + "\n")
# then rerun the bronze orders cell twice

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'as of day 1' AS view, country FROM dim_customer
# MAGIC  WHERE customer_id = 82 AND valid_from <= TIMESTAMP'2026-01-01 12:00' AND (valid_to IS NULL OR valid_to > TIMESTAMP'2026-01-01 12:00')
# MAGIC UNION ALL
# MAGIC SELECT 'current', country FROM dim_customer WHERE customer_id = 82 AND is_current
