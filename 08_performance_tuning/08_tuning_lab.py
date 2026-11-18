# Databricks notebook source
# MAGIC %md
# MAGIC # Project 08: performance tuning lab
# MAGIC Every experiment prints a wall-clock time. Open the **query profile** (serverless) or **Spark UI** (classic) for each one.
# MAGIC Timings vary run to run, so focus on the *order of magnitude* and on the *metrics* (files read, shuffle bytes).

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

import time
from pyspark.sql import functions as F

def timed(label, fn):
    t0 = time.time(); result = fn(); dt = time.time() - t0
    print(f"{label:55s} {dt:7.2f}s"); return result

ROWS = 20_000_000   # lower to 5_000_000 if your Free Edition quota complains

# skewed fact table: customer 1 owns ~30% of rows
facts = (spark.range(ROWS)
         .withColumn("customer_id", F.when(F.rand(1) < 0.3, F.lit(1)).otherwise((F.rand(2) * 100_000).cast("int") + 2))
         .withColumn("product_id", (F.rand(3) * 500).cast("int"))
         .withColumn("event_date", F.date_add(F.lit("2025-01-01"), (F.rand(4) * 365).cast("int")))
         .withColumn("amount", F.round(F.rand(5) * 200, 2))
         .withColumn("country", F.element_at(F.array(*[F.lit(c) for c in ["US","GB","DE","IN","FR","BR","CA","JP"]]),
                                             (F.rand(6) * 8).cast("int") + 1)))
timed("create perf_facts (baseline, no clustering)",
      lambda: facts.write.mode("overwrite").saveAsTable(t("perf_facts")))
dims = spark.range(500).select(F.col("id").cast("int").alias("product_id"),
                               (F.col("id") % 20).cast("string").alias("category"))
dims.write.mode("overwrite").saveAsTable(t("perf_products"))

# COMMAND ----------

# MAGIC %md ## Experiment 1: the small-file problem

# COMMAND ----------

timed("write 2000 small files",
      lambda: spark.table(t("perf_facts")).limit(2_000_000).repartition(2000)
                   .write.mode("overwrite").saveAsTable(t("perf_small_files")))
print("files:", spark.sql(f"DESCRIBE DETAIL {t('perf_small_files')}").first().numFiles)
q = lambda: spark.table(t("perf_small_files")).groupBy("country").agg(F.sum("amount")).collect()
timed("aggregate over 2000 small files", q)
timed("OPTIMIZE", lambda: spark.sql(f"OPTIMIZE {t('perf_small_files')}").collect())
print("files after OPTIMIZE:", spark.sql(f"DESCRIBE DETAIL {t('perf_small_files')}").first().numFiles)
timed("aggregate after OPTIMIZE", q)

# COMMAND ----------

# MAGIC %md ## Experiment 2: data skipping with liquid clustering
# MAGIC Compare **files read / files pruned** in the query profile for the two filtered queries.

# COMMAND ----------

timed("create perf_facts_clustered CLUSTER BY (event_date, customer_id)",
      lambda: spark.sql(f"""CREATE OR REPLACE TABLE {t('perf_facts_clustered')}
                            CLUSTER BY (event_date, customer_id) AS SELECT * FROM {t('perf_facts')}"""))
spark.sql(f"OPTIMIZE {t('perf_facts_clustered')}")   # clusters existing data

sel = "event_date = '2025-06-15' AND customer_id = 4242"
timed("selective query, unclustered", lambda: spark.table(t("perf_facts")).filter(sel).count())
timed("selective query, liquid clustered", lambda: spark.table(t("perf_facts_clustered")).filter(sel).count())

# COMMAND ----------

# MAGIC %md ## Experiment 3: join strategies
# MAGIC Look for `BroadcastHashJoin` vs `SortMergeJoin` in each plan.

# COMMAND ----------

f, p = spark.table(t("perf_facts")), spark.table(t("perf_products"))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # force no broadcast
smj = f.join(p, "product_id").groupBy("category").agg(F.sum("amount"))
smj.explain()
timed("sort-merge join", lambda: smj.collect())
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

bhj = f.join(F.broadcast(p), "product_id").groupBy("category").agg(F.sum("amount"))
bhj.explain()
timed("broadcast hash join", lambda: bhj.collect())

# COMMAND ----------

# MAGIC %md ## Experiment 4: skew
# MAGIC Customer 1 has ~6M rows. A big-big join on `customer_id` puts them all in one partition, unless AQE splits it.
# MAGIC In the query profile / Spark UI stage view, compare max vs median task time.

# COMMAND ----------

cust = spark.range(1, 100_003).select(F.col("id").cast("int").alias("customer_id"),
                                      (F.col("id") % 3).alias("segment"))
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")   # make it a shuffle join so skew matters
skewed = f.join(cust, "customer_id").groupBy("segment").agg(F.sum("amount"))
timed("skewed join (AQE skew handling on)", lambda: skewed.collect())

# Salting: spread the hot key over N sub-keys on the big side, replicate the small side N times
N = 16
f_salted = f.withColumn("salt", (F.rand(7) * N).cast("int"))
cust_salted = cust.crossJoin(spark.range(N).withColumnRenamed("id", "salt").withColumn("salt", F.col("salt").cast("int")))
salted = f_salted.join(cust_salted, ["customer_id", "salt"]).groupBy("segment").agg(F.sum("amount"))
timed("skewed join with manual salting", lambda: salted.collect())
spark.conf.unset("spark.sql.autoBroadcastJoinThreshold")

# COMMAND ----------

# MAGIC %md ## Experiment 5: Python UDF vs pandas UDF vs built-in

# COMMAND ----------

import pandas as pd
from pyspark.sql.types import DoubleType

@F.udf(DoubleType())
def tax_udf(x): return x * 1.2 if x is not None else None

@F.pandas_udf(DoubleType())
def tax_pandas(x: pd.Series) -> pd.Series: return x * 1.2

sample = spark.table(t("perf_facts")).limit(5_000_000)
timed("python UDF", lambda: sample.select(F.sum(tax_udf("amount"))).collect())
timed("pandas UDF", lambda: sample.select(F.sum(tax_pandas("amount"))).collect())
timed("built-in expression", lambda: sample.select(F.sum(F.col("amount") * 1.2)).collect())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Record your results
# MAGIC | Experiment | Before | After | What changed in the plan/profile |
# MAGIC |---|---|---|---|
# MAGIC | Small files → OPTIMIZE | | | files: 2000 → ? |
# MAGIC | Unclustered → liquid clustering | | | files pruned: ? |
# MAGIC | Sort-merge → broadcast | | | Exchange removed on big side |
# MAGIC | Skew: AQE vs salting | | | max task time |
# MAGIC | UDF → pandas UDF → built-in | | | BatchEvalPython / ArrowEvalPython disappear |
# MAGIC
# MAGIC **Interview story template:** "Job X took N minutes. The query profile showed Y. I did Z, and it now takes M minutes and costs P% less."
# MAGIC
# MAGIC Clean up: `DROP TABLE perf_facts; DROP TABLE perf_facts_clustered; DROP TABLE perf_small_files; DROP TABLE perf_products;`
