# Databricks notebook source
# MAGIC %md Bronze: Auto Loader, incremental (`availableNow`), exactly-once via checkpoints.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from pyspark.sql import functions as F

def ingest(source: str, fmt: str, table: str, **opts):
    reader = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", fmt)
              .option("cloudFiles.schemaLocation", f"{SCHEMAS}/{table}")
              .option("cloudFiles.schemaEvolutionMode", "addNewColumns"))
    for k, v in opts.items():
        reader = reader.option(k, v)
    q = (reader.load(f"{LANDING}/{source}")
         .withColumn("_source_file", F.col("_metadata.file_path"))
         .withColumn("_ingested_at", F.current_timestamp())
         .writeStream
         .option("checkpointLocation", f"{CHECKPOINTS}/{table}")
         .option("mergeSchema", "true")
         .trigger(availableNow=True)
         .toTable(t(table)))
    q.awaitTermination()
    n = sum(p["numInputRows"] for p in q.recentProgress)
    print(f"{table}: +{n} rows")
    return n

ingest("orders", "json", "bronze_orders", **{"cloudFiles.inferColumnTypes": "false"})
ingest("customers", "csv", "bronze_customers", header="true", **{"cloudFiles.inferColumnTypes": "true"})

spark.sql(f"""CREATE OR REPLACE TABLE {t('silver_products')} AS
SELECT product_id, name, category, CAST(price AS DECIMAL(10,2)) AS price
FROM read_files('{LANDING}/products', format => 'json')""")
