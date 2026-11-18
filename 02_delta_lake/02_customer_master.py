# Databricks notebook source
# MAGIC %md
# MAGIC # Project 02: customer master table with Delta Lake
# MAGIC MERGE (upserts and deletes), time travel, RESTORE, schema enforcement and evolution, CDF, constraints, OPTIMIZE and VACUUM.

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %run ../common/simulate_arrival

# COMMAND ----------

from pyspark.sql import functions as F, Window
from delta.tables import DeltaTable

land_day(2)   # make sure day 2's customer changes exist

CUSTOMERS = t("customers")
customer_schema = """customer_id INT, first_name STRING, last_name STRING, email STRING, phone STRING,
                     country STRING, loyalty_tier STRING, operation STRING, operation_ts TIMESTAMP"""

def read_cdc(day: int):
    return (spark.read.format("csv").option("header", True).schema(customer_schema)
                 .load(f"{RAW_CUSTOMERS}/customers_{day:03d}.csv"))

# COMMAND ----------

# MAGIC %md ## 1. Initial load (CTAS-style)

# COMMAND ----------

spark.sql(f"DROP TABLE IF EXISTS {CUSTOMERS}")
(read_cdc(1).drop("operation").withColumnRenamed("operation_ts", "updated_at")
    .write.saveAsTable(CUSTOMERS))
spark.sql(f"ALTER TABLE {CUSTOMERS} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")
print(spark.table(CUSTOMERS).count(), "customers")

# COMMAND ----------

# MAGIC %md ## 2. Apply day 2 changes with MERGE
# MAGIC Rule #1 of MERGE: **one source row per key**. Deduplicate first and keep the latest change.

# COMMAND ----------

changes = read_cdc(2)
latest = (changes
          .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("operation_ts"))))
          .filter("rn = 1").drop("rn"))
display(latest.groupBy("operation").count())

# COMMAND ----------

target = DeltaTable.forName(spark, CUSTOMERS)
cols = [c for c in spark.table(CUSTOMERS).columns if c != "updated_at"]

(target.alias("t")
   .merge(latest.alias("s"), "t.customer_id = s.customer_id")
   .whenMatchedDelete(condition="s.operation = 'DELETE'")
   .whenMatchedUpdate(
        condition="s.operation = 'UPDATE' AND s.operation_ts > t.updated_at",   # ignore stale changes
        set={**{c: f"s.{c}" for c in cols}, "updated_at": "s.operation_ts"})
   .whenNotMatchedInsert(
        condition="s.operation != 'DELETE'",
        values={**{c: f"s.{c}" for c in cols}, "updated_at": "s.operation_ts"})
   .execute())

# COMMAND ----------

# MAGIC %md The same MERGE in SQL (for reference, and for the exam):
# MAGIC ```sql
# MAGIC MERGE INTO customers t USING latest_changes s ON t.customer_id = s.customer_id
# MAGIC WHEN MATCHED AND s.operation = 'DELETE' THEN DELETE
# MAGIC WHEN MATCHED AND s.operation = 'UPDATE' AND s.operation_ts > t.updated_at THEN UPDATE SET ...
# MAGIC WHEN NOT MATCHED AND s.operation != 'DELETE' THEN INSERT ...
# MAGIC ```

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {CUSTOMERS}")
             .select("version", "timestamp", "operation", "operationMetrics"))

# COMMAND ----------

# MAGIC %md ## 3. Oops! Time travel and RESTORE
# MAGIC Simulate a bug that overwrites the table with garbage.

# COMMAND ----------

good_version = target.history(1).select("version").first()[0]
spark.table(CUSTOMERS).limit(3).write.mode("overwrite").saveAsTable(CUSTOMERS)   # the "bug"
print("after bug:", spark.table(CUSTOMERS).count())

print("version", good_version, "still has:",
      spark.read.option("versionAsOf", good_version).table(CUSTOMERS).count())

spark.sql(f"RESTORE TABLE {CUSTOMERS} TO VERSION AS OF {good_version}")
print("restored:", spark.table(CUSTOMERS).count())

# COMMAND ----------

# MAGIC %md ## 4. Schema enforcement, then evolution

# COMMAND ----------

bad = spark.createDataFrame([(9999, "x", "y", "e", "p", "US", "GOLD", None, "vip")],
                            "customer_id INT, first_name STRING, last_name STRING, email STRING, phone STRING, "
                            "country STRING, loyalty_tier STRING, updated_at TIMESTAMP, segment STRING")
try:
    bad.write.mode("append").saveAsTable(CUSTOMERS)
except Exception as e:
    print("REJECTED by schema enforcement:\n", str(e)[:300])

# COMMAND ----------

bad.write.mode("append").option("mergeSchema", "true").saveAsTable(CUSTOMERS)   # evolve on purpose
spark.table(CUSTOMERS).printSchema()
spark.sql(f"DELETE FROM {CUSTOMERS} WHERE customer_id = 9999")

# COMMAND ----------

# MAGIC %md ## 5. Change Data Feed: what exactly changed?

# COMMAND ----------

display(spark.read.format("delta").option("readChangeFeed", "true").option("startingVersion", 1)
             .table(CUSTOMERS)
             .groupBy("_commit_version", "_change_type").count().orderBy("_commit_version"))

# COMMAND ----------

# MAGIC %md ## 6. Constraints, OPTIMIZE, VACUUM, and the transaction log

# COMMAND ----------

spark.sql(f"ALTER TABLE {CUSTOMERS} ADD CONSTRAINT valid_tier CHECK (loyalty_tier IN ('BRONZE','SILVER','GOLD'))")
try:
    spark.sql(f"UPDATE {CUSTOMERS} SET loyalty_tier = 'PLATINUM' WHERE customer_id = 1")
except Exception as e:
    print("blocked by constraint:", str(e)[:200])

# COMMAND ----------

display(spark.sql(f"OPTIMIZE {CUSTOMERS}"))
spark.sql(f"VACUUM {CUSTOMERS}")   # default 7-day retention, so nothing recent is removed
display(spark.sql(f"DESCRIBE DETAIL {CUSTOMERS}").select("numFiles", "sizeInBytes", "clusteringColumns", "properties"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn
# MAGIC 1. Land day 3 (`land_day(3)`) and wrap the MERGE in a function `apply_customer_changes(day)`. Make it **idempotent**: running it twice must not change the result.
# MAGIC 2. Write the MERGE in **SQL** using a temp view for the source.
# MAGIC 3. Use CDF to list the emails of customers deleted in the latest version.
# MAGIC 4. Create a `SHALLOW CLONE` named `customers_test`, delete half its rows, and prove the original is untouched.
# MAGIC
# MAGIC ---
# MAGIC ## SOLUTION

# COMMAND ----------

def apply_customer_changes(day: int):
    src = (read_cdc(day)
           .withColumn("rn", F.row_number().over(Window.partitionBy("customer_id").orderBy(F.desc("operation_ts"))))
           .filter("rn = 1").drop("rn"))
    (DeltaTable.forName(spark, CUSTOMERS).alias("t")
        .merge(src.alias("s"), "t.customer_id = s.customer_id")
        .whenMatchedDelete(condition="s.operation = 'DELETE'")
        .whenMatchedUpdate(condition="s.operation = 'UPDATE' AND s.operation_ts > t.updated_at",
                           set={**{c: f"s.{c}" for c in cols}, "updated_at": "s.operation_ts"})
        .whenNotMatchedInsert(condition="s.operation != 'DELETE'",
                              values={**{c: f"s.{c}" for c in cols}, "updated_at": "s.operation_ts"})
        .execute())

land_day(3)
apply_customer_changes(3); n1 = spark.table(CUSTOMERS).count()
apply_customer_changes(3); n2 = spark.table(CUSTOMERS).count()
assert n1 == n2, "not idempotent!"   # updates are guarded by operation_ts, inserts by NOT MATCHED

last = target.history(1).select("version").first()[0]
display(spark.read.option("readChangeFeed", "true").option("startingVersion", last - 1).table(CUSTOMERS)
             .filter("_change_type = 'delete'").select("email", "_commit_version"))

spark.sql(f"CREATE OR REPLACE TABLE {t('customers_test')} SHALLOW CLONE {CUSTOMERS}")
spark.sql(f"DELETE FROM {t('customers_test')} WHERE customer_id % 2 = 0")
print(spark.table(t("customers_test")).count(), "vs original", spark.table(CUSTOMERS).count())
