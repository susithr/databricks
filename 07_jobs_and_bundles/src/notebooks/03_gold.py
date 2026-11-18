# Databricks notebook source
# MAGIC %md Gold: one notebook, parameterised by `table`, so the job runs two tasks in parallel.

# COMMAND ----------

# MAGIC %run ./_common

# COMMAND ----------

from shopstream.transforms import customer_ltv, daily_revenue

dbutils.widgets.text("table", "daily_revenue")
which = dbutils.widgets.get("table")
builders = {"daily_revenue": daily_revenue, "customer_ltv": customer_ltv}

df = builders[which](spark.table(t("silver_orders")))
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t(f"gold_{which}"))
print(f"gold_{which}: {spark.table(t(f'gold_{which}')).count()} rows")
