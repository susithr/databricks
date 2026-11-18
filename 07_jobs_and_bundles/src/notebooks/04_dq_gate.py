# Databricks notebook source
# MAGIC %md Data-quality gate: **fail the job** (so alerts fire) if too many new rows were quarantined.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

dbutils.widgets.text("max_bad_pct", "10")
max_bad_pct = float(dbutils.widgets.get("max_bad_pct"))

last = spark.sql(f"""
  SELECT sum(total) AS total, sum(bad) AS bad FROM {t('dq_batches')}
  WHERE run_at >= current_timestamp() - INTERVAL 2 HOURS""").first()

total, bad = (last.total or 0), (last.bad or 0)
pct = 100 * bad / total if total else 0.0
print(f"this run: {bad}/{total} rows quarantined ({pct:.2f}%), threshold {max_bad_pct}%")

landed = dbutils.jobs.taskValues.get(taskKey="land_data", key="landed_day", debugValue=0)
if landed and total == 0:
    raise Exception(f"day {landed} was landed but silver processed 0 rows: is ingestion broken?")
if pct > max_bad_pct:
    raise Exception(f"DQ gate failed: {pct:.2f}% bad rows > {max_bad_pct}%")
