# Databricks notebook source
# MAGIC %md Silver: clean, quarantine and MERGE new Bronze rows (streaming `foreachBatch`).

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from shopstream.transforms import clean_orders, enrich_orders, latest_per_key, split_valid

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {t('silver_orders')} (
  order_id STRING NOT NULL, customer_id INT, product_id STRING, product_name STRING, category STRING,
  quantity INT, unit_price DECIMAL(10,2), amount DECIMAL(12,2), status STRING, channel STRING,
  order_ts TIMESTAMP, updated_ts TIMESTAMP, order_date DATE, _ingested_at TIMESTAMP)
CLUSTER BY (order_date)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('quarantine_orders')}
  (order_id STRING, dq_errors ARRAY<STRING>, _source_file STRING, _ingested_at TIMESTAMP)""")
spark.sql(f"""CREATE TABLE IF NOT EXISTS {t('dq_batches')}
  (batch_id BIGINT, run_at TIMESTAMP, total BIGINT, bad BIGINT)""")


def process(batch_df, batch_id):
    cleaned = clean_orders(batch_df)
    valid, bad = split_valid(cleaned)
    bad.select("order_id", "dq_errors", "_source_file", "_ingested_at") \
       .write.mode("append").saveAsTable(t("quarantine_orders"))

    src = enrich_orders(latest_per_key(valid, "order_id", "updated_ts"), spark.table(t("silver_products")))
    cols = spark.table(t("silver_orders")).columns
    (DeltaTable.forName(spark, t("silver_orders")).alias("t")
        .merge(src.select(*cols).alias("s"), "t.order_id = s.order_id")
        .whenMatchedUpdateAll(condition="s.updated_ts > t.updated_ts")
        .whenNotMatchedInsertAll()
        .execute())

    stats = cleaned.agg(F.count("*").alias("total"),
                        F.sum((F.size("dq_errors") > 0).cast("int")).alias("bad")).first()
    spark.createDataFrame([(batch_id, stats.total, stats.bad or 0)], "batch_id BIGINT, total BIGINT, bad BIGINT") \
         .withColumn("run_at", F.current_timestamp()) \
         .select("batch_id", "run_at", "total", "bad") \
         .write.mode("append").saveAsTable(t("dq_batches"))


q = (spark.readStream.table(t("bronze_orders"))
     .writeStream.foreachBatch(process)
     .option("checkpointLocation", f"{CHECKPOINTS}/silver_orders")
     .trigger(availableNow=True).start())
q.awaitTermination()
