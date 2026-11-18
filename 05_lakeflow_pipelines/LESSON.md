# Module 05 — Lakeflow Declarative Pipelines (formerly Delta Live Tables / DLT)

## 1. Why declarative?
In Modules 03–04 you hand-wrote checkpoints, MERGEs, SCD2 logic, quarantine tables and run order.
With **Lakeflow Declarative Pipelines** (LDP, previously "DLT"), you declare **what each table is**, and the engine handles:
dependency order (a DAG), incremental processing, checkpoints, retries, schema evolution, data-quality metrics, and lineage.

> Naming: Databricks renamed DLT to Lakeflow Declarative Pipelines in 2025, and the engine was open-sourced as
> **Spark Declarative Pipelines**. The new Python module is `from pyspark import pipelines as dp`.
> The old `import dlt` still works, and older docs and exam questions use it. Both are shown below.

## 2. The three dataset types
| Type | Python (`dp`) | Legacy `dlt` | SQL | Semantics |
|---|---|---|---|---|
| **Streaming table** | `@dp.table` returning `spark.readStream...` | `@dlt.table` | `CREATE OR REFRESH STREAMING TABLE` | Incremental, append-style. Each input row processed once. Bronze/Silver. |
| **Materialized view** | `@dp.materialized_view` | `@dlt.table` returning batch DF | `CREATE OR REFRESH MATERIALIZED VIEW` | Result always correct w.r.t. inputs. Recomputed incrementally when possible. Gold. |
| **Temporary view** | `@dp.temporary_view` | `@dlt.view` | `CREATE TEMPORARY VIEW` | Intermediate, not published. |

## 3. Expectations (data quality)
```python
@dp.expect("valid_channel", "channel IN ('web','app','store')")       # keep row, record metric
@dp.expect_or_drop("positive_qty", "quantity > 0")                    # drop row
@dp.expect_or_fail("has_id", "order_id IS NOT NULL")                  # stop the update
@dp.expect_all_or_drop({"a": "...", "b": "..."})                      # many at once
```
```sql
CONSTRAINT positive_qty EXPECT (quantity > 0) ON VIOLATION DROP ROW
```
Quarantine pattern: put the *inverse* of your rules into a separate table so bad rows are kept, not just counted.

## 4. CDC with AUTO CDC (formerly APPLY CHANGES)
Handles out-of-order events, deletes, and SCD1 or SCD2, with no MERGE code to write:
```python
dp.create_streaming_table("dim_customer")
dp.create_auto_cdc_flow(
    target="dim_customer", source="customers_cdc_clean",
    keys=["customer_id"], sequence_by="operation_ts",
    apply_as_deletes="operation = 'DELETE'",
    except_column_list=["operation", "_rescued_data"],
    stored_as_scd_type=2)
# legacy: dlt.apply_changes(...) with the same arguments
```
```sql
CREATE FLOW customers_flow AS AUTO CDC INTO dim_customer FROM STREAM(customers_cdc_clean)
KEYS (customer_id) APPLY AS DELETE WHEN operation = 'DELETE' SEQUENCE BY operation_ts
COLUMNS * EXCEPT (operation, _rescued_data) STORED AS SCD TYPE 2;
-- legacy: APPLY CHANGES INTO LIVE.dim_customer FROM STREAM(LIVE.customers_cdc_clean) ...
```
SCD2 tables get `__START_AT` / `__END_AT` columns automatically.

## 5. Running pipelines
- Create a pipeline in the UI: *Jobs & Pipelines → Create → ETL pipeline*. Point it at the source file(s), choose catalog and schema, **serverless**.
- **Development** mode reuses compute and doesn't retry, so it's fast to iterate. **Production** retries and terminates compute.
- **Triggered** (run once, like `availableNow`) vs **Continuous** (always on).
- **Full refresh** recomputes everything (careful: streaming tables re-read all source data).
- Pipeline settings → **Configuration** key/values are read with `spark.conf.get("key")`.
- The **event log** (`event_log(TABLE(...))` or the UI) has DQ metrics, lineage and timings.

## 6. Project 05: declarative ShopStream
Files: [`shopstream_pipeline.py`](shopstream_pipeline.py) (Python) and [`shopstream_gold.sql`](shopstream_gold.sql) (SQL). One pipeline can mix both.

Steps:
1. *Jobs & Pipelines → Create → ETL pipeline*. Name: `shopstream_ldp`. Catalog `workspace`, schema **`shopstream_ldp`** (a new schema so it doesn't collide with earlier modules).
2. Add both files as source code.
3. Configuration: `landing_path = /Volumes/workspace/shopstream/landing`.
4. Click **Start**. Explore the graph, then click each table to see expectation pass/fail counts.
5. Land another day (run `land_day(4)` in any notebook), then **Start** again. Only new data is processed.
6. Run [`05_explore_results.py`](05_explore_results.py) to query outputs and the event log.

### Your turn
- Add an expectation `valid_channel` (warn only) and find its metrics in the event log.
- Convert `gold_daily_revenue` from SQL to Python, or the reverse.
- Change `dim_customer` to SCD type 1 and compare the row count.
- Compare lines of code: Module 04 vs this module. That's the selling point of LDP.
