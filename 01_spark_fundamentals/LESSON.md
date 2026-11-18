# Module 01 — Apache Spark fundamentals (DataFrames & Spark SQL)

## 1. Mental model
- Spark is a **distributed** engine: data is split into **partitions**, and each partition is processed by a **task** on an executor core.
- You write **transformations** (`select`, `filter`, `join`, `groupBy`) that are **lazy**: they only build a plan.
- An **action** (`count`, `collect`, `display`, `write`, `show`) triggers execution.
- **Narrow** transformations (`filter`, `select`, `withColumn`) need no data movement.
  **Wide** transformations (`groupBy`, `join`, `distinct`, `orderBy`) need a **shuffle** (data moves between executors over the network). Shuffles are the #1 cost.
- Execution hierarchy: **Job** (one per action) → **Stages** (split at shuffles) → **Tasks** (one per partition).
- The **Catalyst optimizer** rewrites your plan (predicate pushdown, column pruning, join reordering). `df.explain(True)` shows the plans.
  **AQE** (Adaptive Query Execution, on by default) re-optimizes at runtime using real statistics.

## 2. The API you'll use 95% of the time
```python
from pyspark.sql import functions as F, Window

df = spark.read.table("samples.nyctaxi.trips")              # read a table
df = spark.read.format("json").load("/Volumes/.../orders")  # read files
df.printSchema(); df.columns; df.dtypes

(df.select("a", F.col("b").alias("bb"), (F.col("x") * 2).alias("x2"))
   .filter((F.col("x") > 0) & F.col("y").isNotNull())
   .withColumn("day", F.to_date("ts"))
   .withColumnRenamed("old", "new")
   .drop("tmp")
   .dropDuplicates(["id"])
   .groupBy("day").agg(F.count("*").alias("n"), F.sum("amt").alias("total"))
   .orderBy(F.desc("total")))

df.join(other, on="id", how="left")      # inner, left, right, full, left_semi, left_anti, cross
F.broadcast(small_df)                     # hint: ship small table to all executors and skip the shuffle
F.when(cond, v).otherwise(v2); F.coalesce(a, b); F.lit(1); F.expr("sql expr")
F.to_timestamp("s", "dd/MM/yyyy HH:mm:ss"); F.date_trunc("hour", ts); F.datediff(a, b)
F.explode(arr); F.from_json(col, schema); F.col("struct.field"); F.split(s, ",")

w = Window.partitionBy("customer").orderBy(F.desc("ts"))
df.withColumn("rn", F.row_number().over(w))   # also rank, dense_rank, lag, lead, sum().over(w)

df.createOrReplaceTempView("v"); spark.sql("SELECT * FROM v")   # mix Python and SQL freely
df.write.mode("overwrite").saveAsTable("catalog.schema.table")  # modes: append, overwrite, error, ignore
```

### Schemas
Inferring schemas is fine for exploring. In production, **declare them** (faster and safer):
```python
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
schema = StructType([StructField("id", StringType(), False), StructField("qty", IntegerType())])
# or as a DDL string: "id STRING NOT NULL, qty INT"
```

### UDFs
Prefer built-in functions. Python UDFs serialize every row to Python, which is slow. If you must, use
**pandas UDFs** (`@F.pandas_udf`), which are vectorized with Arrow.

## 3. Project 01: NYC taxi trip analytics
Notebook: [`01_taxi_analytics.py`](01_taxi_analytics.py), using the built-in `samples.nyctaxi.trips` table.
You will:
1. Explore the schema and profile the data (nulls, ranges, outliers).
2. Clean it: remove impossible trips, derive duration, speed, hour and weekday.
3. Answer business questions with the DataFrame API **and** SQL (busiest hours, revenue by day, top routes).
4. Use window functions: each pickup zip's top 3 destinations, and a running daily total.
5. Read `explain()` output and the Spark UI to see shuffles.
6. Save the result as a managed Delta table.

### Your turn (exercises are in the notebook)
- Find the 5 zip pairs with the highest average fare per mile (minimum 50 trips).
- Compute day-over-day % change in revenue with `F.lag`.
- Re-write question 3 in pure SQL and compare `explain()` output. Is the plan the same? (It should be: same Catalyst.)
