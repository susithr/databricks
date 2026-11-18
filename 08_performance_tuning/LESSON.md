# Module 08 — Performance tuning & cost optimization

## 1. Where time goes (diagnose before you tune)
Open the **Spark UI** (classic) or **Query Profile** (serverless/SQL). Look for:
| Symptom | Likely cause | Fix |
|---|---|---|
| Huge **shuffle read/write** | wide joins/aggregations | broadcast the small side, pre-aggregate, filter earlier |
| One task 10× slower than the rest | **data skew** | AQE skew join (on by default), salting, isolate hot keys |
| **Spill** to disk | partitions too big for memory | more shuffle partitions / AQE, bigger nodes |
| Thousands of tiny tasks / files | **small-file problem** | OPTIMIZE, optimized writes, auto compaction, fewer partitions |
| Reading all files for a selective filter | no data skipping | liquid clustering on filter columns, stats on first 32 cols |
| Python UDF in plan (`BatchEvalPython`) | row-at-a-time Python | built-in functions or pandas UDF |

## 2. Data layout
- **Liquid clustering** (`CLUSTER BY (col1, col2)`) is the modern default. It replaces partitioning + Z-ORDER, is incremental, and you can change keys anytime. `CLUSTER BY AUTO` lets Databricks choose keys from query patterns.
- **Partitioning** (`PARTITIONED BY (date)`): only for very large tables (TB+) with low-cardinality columns. Over-partitioning creates small files and is a classic mistake.
- **Z-ORDER**: legacy multi-dimensional clustering via `OPTIMIZE ... ZORDER BY`. Can't be combined with liquid clustering.
- **File size**: target ~128 MB–1 GB. `OPTIMIZE` compacts. **Predictive optimization** runs OPTIMIZE/VACUUM/ANALYZE automatically on UC managed tables.
- **Statistics**: `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` helps the optimizer choose join strategies.

## 3. Spark execution knobs
- **AQE** (`spark.sql.adaptive.enabled`, default on): coalesces shuffle partitions, switches to broadcast joins at runtime, splits skewed partitions.
- **Shuffle partitions** default 200. AQE auto-tunes. On classic you can set `spark.sql.shuffle.partitions=auto` (Databricks).
- **Join strategies**: broadcast hash (small side < `spark.sql.autoBroadcastJoinThreshold`, 10 MB default; hint with `F.broadcast`) → shuffle hash → sort-merge (big–big).
- **Photon**: vectorized engine, big wins on SQL/DataFrame workloads. Always on for serverless.
- **Caching**: the Delta/disk cache is automatic on many node types. `df.cache()` is rarely needed (and unavailable on serverless).
- **Avoid** `collect()` on big data, `count()` just for logging in loops, `.toPandas()` on large DataFrames, and reading the same source twice.

## 4. Cost levers
Serverless or **job compute** instead of all-purpose for production · autoscaling + auto-termination · spot instances for workers ·
`availableNow` streaming instead of 24/7 · right-size SQL warehouses · watch `system.billing.usage`.

## 5. Project 08: make a slow job fast
Notebook: [`08_tuning_lab.py`](08_tuning_lab.py). Generates a ~20M-row skewed fact table and runs experiments, timing each:
1. **Small files:** write 2,000 tiny files → query → `OPTIMIZE` → query again.
2. **Data skipping:** selective filter on an unclustered table vs a **liquid-clustered** table (compare files read in the query profile).
3. **Join strategies:** sort-merge vs broadcast join (`explain()`).
4. **Skew:** one customer owns 30% of rows. See AQE split it, then try salting.
5. **UDF vs built-in:** Python UDF vs pandas UDF vs native function.

Write down your timings in the table at the end. That table is great material for interview stories.
