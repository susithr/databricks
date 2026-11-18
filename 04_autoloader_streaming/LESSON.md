# Module 04 — Auto Loader & Structured Streaming

## 1. Structured Streaming in one paragraph
A stream is an **unbounded table**. You write the same DataFrame code as for batch. Spark runs it as a series of
**micro-batches**, and a **checkpoint** records what has been processed. Restarting from the checkpoint gives
**exactly-once** results into Delta. Same code, incremental execution.

```python
(spark.readStream.<source>
   .transformations...
 .writeStream
   .option("checkpointLocation", path)    # REQUIRED, one per stream, never shared
   .trigger(availableNow=True)             # process everything new, then stop (incremental batch)
   # .trigger(processingTime="1 minute")   # always-on, micro-batch every minute
   .outputMode("append")                   # append | update | complete
   .toTable("catalog.schema.table"))
```
**`availableNow`** is the most common production pattern: a scheduled job that starts, processes only new data, and
shuts down. That gives streaming guarantees at batch prices.

## 2. Auto Loader (`cloudFiles`)
Incrementally and efficiently ingests new files from cloud storage. It's the standard for file ingestion into Bronze.
```python
(spark.readStream.format("cloudFiles")
   .option("cloudFiles.format", "json")                         # json, csv, parquet, avro, text, binaryFile...
   .option("cloudFiles.schemaLocation", schema_path)            # where the inferred schema is tracked
   .option("cloudFiles.inferColumnTypes", "true")
   .option("cloudFiles.schemaEvolutionMode", "addNewColumns")  # or rescue / failOnNewColumns / none
   .option("cloudFiles.schemaHints", "quantity INT")
   .load(path))
```
- **`_rescued_data`** column: values that didn't fit the schema (wrong type, unexpected column) end up here instead of being lost.
- **Directory listing** (default) vs **file notification** mode (cloud queue events: scales to millions of files).
- Auto Loader vs `COPY INTO`: both are idempotent. Auto Loader scales better and handles schema evolution. Use COPY INTO for simple SQL-only loads of thousands of files.
- When a new column appears with `addNewColumns`, the stream **fails once on purpose**, updates the schema, and works on restart. In Jobs, set retries.

## 3. Stateful streaming
- **Aggregations** keep state across micro-batches. **Watermarks** (`withWatermark("ts", "10 minutes")`) let Spark drop old state and handle late data.
- **Stream-static join**: join a stream to a Delta table (the static side is re-read each micro-batch).
- **Stream-stream join**: needs watermarks on both sides.
- **`dropDuplicatesWithinWatermark`**: dedupe events within the watermark.

## 4. `foreachBatch`: the Swiss-army knife
Streams can't MERGE directly. With `foreachBatch` you get each micro-batch as a normal DataFrame:
```python
def upsert(batch_df, batch_id):
    DeltaTable.forName(spark, "silver").alias("t").merge(batch_df.alias("s"), "...")...execute()
stream.writeStream.foreachBatch(upsert).option("checkpointLocation", cp).trigger(availableNow=True).start()
```

## 5. Slowly Changing Dimensions
| Type | Behaviour |
|---|---|
| **SCD1** | Overwrite. No history (`customers` in Module 02). |
| **SCD2** | Keep history: close the old row (`valid_to`, `is_current = false`) and insert a new current row. |

SCD2 MERGE trick: union the source with itself. One copy has a NULL merge key so it always INSERTs the new version,
and the other copy UPDATEs (closes) the current row.

## 6. Project 04: incremental ShopStream
Notebook: [`04_incremental_ingestion.py`](04_incremental_ingestion.py)
1. **Bronze with Auto Loader**: orders (JSON) and customer CDC (CSV), with `_rescued_data` and file metadata.
2. **Silver orders with `foreachBatch` + MERGE**: dedupe and late updates, incrementally.
3. **Silver `dim_customer` as SCD Type 2** from the CDC stream.
4. Land a new day and rerun: only the new files are processed (check `numInputRows` in the progress).
5. **Streaming aggregation with a watermark** for orders per 1-hour window.

### Your turn
- Land a file with a **new column** (e.g. `coupon_code`) and watch schema evolution happen.
- Query: "what was customer 82's country on day 1 vs today?" using the SCD2 table.
- Change the trigger to `processingTime="10 seconds"`, land 2 more days while it runs, then stop it with `query.stop()`.
