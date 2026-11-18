# Databricks notebook source
# MAGIC %md
# MAGIC # Project 03: ShopStream medallion pipeline (batch)
# MAGIC Bronze (raw) → Silver (clean + quarantine + MERGE) → Gold (KPIs). Fully idempotent.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/simulate_arrival

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath("../common"))
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from transforms import clean_orders, split_valid, latest_per_key, enrich_orders, daily_revenue, customer_ltv

dbutils.widgets.text("land_day", "2", "Land this day before running (0 = none)")
day = int(dbutils.widgets.get("land_day"))
if day:
    land_day(day)
print(landed_days())

# COMMAND ----------

# MAGIC %md ## BRONZE: idempotent raw load with COPY INTO
# MAGIC Every column is a STRING in Bronze, so we never fail on bad types. COPY INTO skips files it already loaded.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('bronze_orders')} (
  order_id STRING, customer_id STRING, product_id STRING, quantity STRING, unit_price STRING,
  status STRING, order_ts STRING, updated_ts STRING, channel STRING,
  _source_file STRING, _ingested_at TIMESTAMP)
""")

display(spark.sql(f"""
COPY INTO {t('bronze_orders')}
FROM (SELECT order_id, customer_id, product_id, quantity, unit_price, status, order_ts, updated_ts, channel,
             _metadata.file_path AS _source_file, current_timestamp() AS _ingested_at
      FROM '{RAW_ORDERS}')
FILEFORMAT = JSON
FORMAT_OPTIONS ('primitivesAsString' = 'true')
"""))

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {t('silver_products')} AS
SELECT product_id, name, category, CAST(price AS DECIMAL(10,2)) AS price
FROM read_files('{RAW_PRODUCTS}', format => 'json')
""")

# COMMAND ----------

# MAGIC %md ## SILVER: clean, quarantine, dedupe, MERGE
# MAGIC We only process Bronze rows from files Silver hasn't seen yet (a simple high-water mark on `_ingested_at`).
# MAGIC Module 04 replaces this with streaming checkpoints.

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('silver_orders')} (
  order_id STRING NOT NULL, customer_id INT, product_id STRING, product_name STRING, category STRING,
  quantity INT, unit_price DECIMAL(10,2), amount DECIMAL(12,2), status STRING, channel STRING,
  order_ts TIMESTAMP, updated_ts TIMESTAMP, order_date DATE, _ingested_at TIMESTAMP)
""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('quarantine_orders')}
              (order_id STRING, dq_errors ARRAY<STRING>, raw STRING, _source_file STRING, _ingested_at TIMESTAMP)""")

hwm = spark.sql(f"SELECT coalesce(max(_ingested_at), TIMESTAMP'1900-01-01') FROM {t('silver_orders')}").first()[0]
new_bronze = spark.table(t("bronze_orders")).filter(F.col("_ingested_at") > F.lit(hwm))
print("new bronze rows:", new_bronze.count())

# COMMAND ----------

cleaned = clean_orders(new_bronze)
valid, bad = split_valid(cleaned)

(bad.select("order_id", "dq_errors",
            F.to_json(F.struct(*new_bronze.columns[:9])).alias("raw"), "_source_file", "_ingested_at")
    .write.mode("append").saveAsTable(t("quarantine_orders")))

silver_batch = enrich_orders(latest_per_key(valid, "order_id", "updated_ts"),
                             spark.table(t("silver_products")))
silver_cols = spark.table(t("silver_orders")).columns

(DeltaTable.forName(spark, t("silver_orders")).alias("t")
    .merge(silver_batch.select(*silver_cols).alias("s"), "t.order_id = s.order_id")
    .whenMatchedUpdateAll(condition="s.updated_ts > t.updated_ts")   # only newer versions win
    .whenNotMatchedInsertAll()
    .execute())

display(spark.sql(f"DESCRIBE HISTORY {t('silver_orders')} LIMIT 1").select("operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md ## GOLD: business tables
# MAGIC Small enough to rebuild fully each run (still idempotent). At scale you'd MERGE only affected dates.

# COMMAND ----------

silver = spark.table(t("silver_orders"))
daily_revenue(silver).write.mode("overwrite").saveAsTable(t("gold_daily_revenue"))
customer_ltv(silver).write.mode("overwrite").saveAsTable(t("gold_customer_ltv"))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT order_date, category, orders, units, revenue
# MAGIC FROM gold_daily_revenue ORDER BY order_date, revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Data quality at a glance
# MAGIC SELECT err, count(*) AS n FROM quarantine_orders LATERAL VIEW explode(dq_errors) e AS err GROUP BY err ORDER BY n DESC

# COMMAND ----------

for name in ["bronze_orders", "silver_orders", "quarantine_orders", "gold_daily_revenue", "gold_customer_ltv"]:
    print(f"{name:22s} {spark.table(t(name)).count():>8}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn
# MAGIC 1. `gold_channel_mix`: per day, each channel's revenue and % share.
# MAGIC 2. DQ report: % quarantined per error type per source file.
# MAGIC 3. Run this notebook again with `land_day = 0`. Assert no table changed size. Why does Bronze not duplicate? (COPY INTO file tracking.)
# MAGIC 4. Set `land_day = 3` and rerun. Find an order whose status changed from PLACED to something else and show both versions using `VERSION AS OF`.
# MAGIC
# MAGIC ---
# MAGIC ## SOLUTION

# COMMAND ----------

from pyspark.sql import Window
mix = (silver.filter(F.col("status").isin("PLACED", "SHIPPED", "DELIVERED"))
             .groupBy("order_date", "channel").agg(F.sum("amount").alias("revenue"))
             .withColumn("share_pct", F.round(F.col("revenue") / F.sum("revenue").over(Window.partitionBy("order_date")) * 100, 1)))
mix.write.mode("overwrite").saveAsTable(t("gold_channel_mix"))
display(mix.orderBy("order_date", "channel"))

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH totals AS (SELECT _source_file, count(*) AS total FROM bronze_orders GROUP BY ALL),
# MAGIC      errs AS (SELECT _source_file, err, count(*) AS n FROM quarantine_orders LATERAL VIEW explode(dq_errors) e AS err GROUP BY ALL)
# MAGIC SELECT e._source_file, err, n, round(n / total * 100, 2) AS pct
# MAGIC FROM errs e JOIN totals USING (_source_file) ORDER BY 1, pct DESC
