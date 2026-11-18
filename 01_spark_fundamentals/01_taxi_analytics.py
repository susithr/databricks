# Databricks notebook source
# MAGIC %md
# MAGIC # Project 01: NYC taxi trip analytics
# MAGIC **Goal:** get fluent with DataFrames, Spark SQL, joins, aggregations and window functions.
# MAGIC Dataset: `samples.nyctaxi.trips` (built into every Databricks workspace).

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

from pyspark.sql import functions as F, Window

trips = spark.read.table("samples.nyctaxi.trips")
trips.printSchema()
display(trips.limit(10))

# COMMAND ----------

# MAGIC %md ## Step 1: profile the data
# MAGIC Always look before you transform. How many rows? Nulls? Crazy values?

# COMMAND ----------

print("rows:", trips.count())   # an ACTION: Spark actually runs here

display(trips.select(
    *[F.sum(F.col(c).isNull().cast("int")).alias(f"null_{c}") for c in trips.columns]
))
display(trips.select("trip_distance", "fare_amount").summary("min", "25%", "50%", "75%", "max"))

# COMMAND ----------

# MAGIC %md ## Step 2: clean and enrich
# MAGIC Nothing runs yet. These are lazy **transformations**.

# COMMAND ----------

clean = (
    trips
    .withColumn("duration_min",
                (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60)
    .filter(
        (F.col("trip_distance") > 0) & (F.col("trip_distance") < 100) &
        (F.col("fare_amount") > 0) & (F.col("fare_amount") < 500) &
        (F.col("duration_min") > 1) & (F.col("duration_min") < 240)
    )
    .withColumn("speed_mph", F.round(F.col("trip_distance") / (F.col("duration_min") / 60), 1))
    .filter(F.col("speed_mph") < 80)
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
    .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
    .withColumn("weekday", F.date_format("tpep_pickup_datetime", "E"))
    .withColumn("is_weekend", F.dayofweek("tpep_pickup_datetime").isin(1, 7))
    .withColumn("fare_per_mile", F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
)
print(f"kept {clean.count()} of {trips.count()} trips")

# COMMAND ----------

# MAGIC %md ## Step 3: business questions (DataFrame API)

# COMMAND ----------

# Q1: busiest pickup hours
display(clean.groupBy("pickup_hour").agg(F.count("*").alias("trips")).orderBy("pickup_hour"))

# COMMAND ----------

# Q2: revenue and average trip by day
daily = (clean.groupBy("pickup_date")
              .agg(F.count("*").alias("trips"),
                   F.round(F.sum("fare_amount"), 2).alias("revenue"),
                   F.round(F.avg("trip_distance"), 2).alias("avg_miles"))
              .orderBy("pickup_date"))
display(daily)

# COMMAND ----------

# Q3: weekday vs weekend behaviour
display(clean.groupBy("is_weekend").agg(
    F.round(F.avg("fare_amount"), 2).alias("avg_fare"),
    F.round(F.avg("duration_min"), 1).alias("avg_minutes"),
    F.round(F.avg("speed_mph"), 1).alias("avg_speed")))

# COMMAND ----------

# MAGIC %md ## Step 4: the same in SQL
# MAGIC A temp view lets SQL and Python share a DataFrame. Both compile to the same plan.

# COMMAND ----------

clean.createOrReplaceTempView("clean_trips")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Q4: top routes (pickup zip -> dropoff zip)
# MAGIC SELECT pickup_zip, dropoff_zip, count(*) AS trips, round(avg(fare_amount), 2) AS avg_fare
# MAGIC FROM clean_trips
# MAGIC GROUP BY ALL
# MAGIC ORDER BY trips DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md ## Step 5: window functions
# MAGIC A window computes over a group of rows **without collapsing them** (unlike `groupBy`).

# COMMAND ----------

routes = clean.groupBy("pickup_zip", "dropoff_zip").agg(F.count("*").alias("trips"))
w = Window.partitionBy("pickup_zip").orderBy(F.desc("trips"))
top3 = (routes.withColumn("rank", F.dense_rank().over(w))
              .filter("rank <= 3")
              .orderBy("pickup_zip", "rank"))
display(top3)

# COMMAND ----------

# running total of revenue + 7-day moving average
w_run = Window.orderBy("pickup_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_7d = Window.orderBy("pickup_date").rowsBetween(-6, 0)
display(daily.withColumn("running_revenue", F.round(F.sum("revenue").over(w_run), 2))
             .withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(w_7d), 2)))

# COMMAND ----------

# MAGIC %md ## Step 6: look under the hood
# MAGIC Find `Exchange` in the physical plan. That's a **shuffle**. Then run the cell and open the
# MAGIC *Spark Jobs → View* link (or *Query profile* on serverless) to see the stages.

# COMMAND ----------

top3.explain(mode="formatted")

# COMMAND ----------

# MAGIC %md ## Step 7: persist the result as a Delta table

# COMMAND ----------

(clean.write.mode("overwrite").option("overwriteSchema", "true")
      .saveAsTable(t("taxi_trips_clean")))
display(spark.sql(f"DESCRIBE DETAIL {t('taxi_trips_clean')}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn
# MAGIC 1. The 5 zip pairs with the highest **average fare per mile** (min 50 trips).
# MAGIC 2. **Day-over-day % change** in revenue using `F.lag`.
# MAGIC 3. For each weekday, the hour with the most trips (hint: window + `row_number`).
# MAGIC 4. Write Q3 (weekday vs weekend) in SQL and compare `EXPLAIN` output with the Python version.
# MAGIC
# MAGIC Try them before scrolling.
# MAGIC
# MAGIC ---
# MAGIC ## SOLUTION

# COMMAND ----------

# 1
display(clean.groupBy("pickup_zip", "dropoff_zip")
             .agg(F.count("*").alias("n"), F.round(F.avg("fare_per_mile"), 2).alias("avg_fpm"))
             .filter("n >= 50").orderBy(F.desc("avg_fpm")).limit(5))

# 2
w_lag = Window.orderBy("pickup_date")
display(daily.withColumn("prev", F.lag("revenue").over(w_lag))
             .withColumn("dod_pct", F.round((F.col("revenue") - F.col("prev")) / F.col("prev") * 100, 1)))

# 3
by_hour = clean.groupBy("weekday", "pickup_hour").agg(F.count("*").alias("n"))
display(by_hour.withColumn("rn", F.row_number().over(Window.partitionBy("weekday").orderBy(F.desc("n"))))
               .filter("rn = 1").drop("rn"))
