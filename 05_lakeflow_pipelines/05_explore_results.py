# Databricks notebook source
# MAGIC %md
# MAGIC # Project 05: explore the pipeline's output and event log
# MAGIC Run this after the `shopstream_ldp` pipeline has completed at least one update.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE workspace.shopstream_ldp;
# MAGIC SHOW TABLES;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold_daily_revenue ORDER BY order_date, revenue DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- SCD2 history created automatically by AUTO CDC
# MAGIC SELECT customer_id, country, loyalty_tier, __START_AT, __END_AT
# MAGIC FROM dim_customer
# MAGIC WHERE customer_id IN (SELECT customer_id FROM dim_customer GROUP BY 1 HAVING count(*) > 1)
# MAGIC ORDER BY customer_id, __START_AT LIMIT 20

# COMMAND ----------

# MAGIC %md ## Data-quality metrics from the event log
# MAGIC Every pipeline writes an event log. `event_log(TABLE(...))` reads it for any table the pipeline owns.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   timestamp,
# MAGIC   origin.flow_name,
# MAGIC   exp.name AS expectation,
# MAGIC   exp.passed_records,
# MAGIC   exp.failed_records
# MAGIC FROM event_log(TABLE(workspace.shopstream_ldp.silver_order_events)),
# MAGIC LATERAL explode(from_json(details:flow_progress:data_quality:expectations,
# MAGIC        'array<struct<name:string,dataset:string,passed_records:int,failed_records:int>>')) AS t(exp)
# MAGIC WHERE event_type = 'flow_progress' AND details:flow_progress:data_quality IS NOT NULL
# MAGIC ORDER BY timestamp DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- why rows were quarantined
# MAGIC SELECT
# MAGIC   CASE WHEN order_id IS NULL THEN 'no_id' WHEN customer_id IS NULL THEN 'no_customer'
# MAGIC        WHEN quantity IS NULL OR quantity <= 0 THEN 'bad_qty' WHEN order_ts IS NULL THEN 'bad_ts' ELSE 'price' END AS reason,
# MAGIC   count(*)
# MAGIC FROM quarantine_orders GROUP BY 1
