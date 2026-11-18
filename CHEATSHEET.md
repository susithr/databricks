# Databricks Data Engineer Cheat Sheet

## Reading
```python
spark.read.table("cat.sch.tbl")
spark.read.format("json|csv|parquet|delta").option("header", True).schema(ddl).load(path)
spark.sql("SELECT * FROM read_files('/Volumes/c/s/v/dir', format => 'csv', header => true)")
spark.read.option("versionAsOf", 5).table(t)                 # time travel
spark.read.option("readChangeFeed", "true").option("startingVersion", 3).table(t)   # CDF
df.select("*", "_metadata.file_path", "_metadata.file_modification_time")
```

## Writing
```python
df.write.mode("append|overwrite").saveAsTable(t)
df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(t)
df.write.mode("append").option("mergeSchema", "true").saveAsTable(t)
df.write.mode("overwrite").option("replaceWhere", "d >= '2026-01-01'").saveAsTable(t)
```

## MERGE (Python)
```python
from delta.tables import DeltaTable
(DeltaTable.forName(spark, t).alias("t").merge(src.alias("s"), "t.id = s.id")
  .whenMatchedDelete("s.op = 'D'")
  .whenMatchedUpdateAll("s.ts > t.ts")
  .whenNotMatchedInsertAll()
  .whenNotMatchedBySourceDelete()      # optional
  .execute())
```

## Streaming
```python
(spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
   .option("cloudFiles.schemaLocation", sl).load(path)
 .writeStream.option("checkpointLocation", cp).trigger(availableNow=True).toTable(t))

spark.readStream.table(t)                                  # Delta as a source (append-only)
spark.readStream.option("skipChangeCommits", "true").table(t)
.withWatermark("ts", "10 minutes").groupBy(F.window("ts", "5 minutes")).count()
.writeStream.foreachBatch(lambda df, id: ...)              # MERGE per micro-batch
q.status; q.lastProgress; q.recentProgress; q.stop(); q.awaitTermination()
```

## Delta maintenance SQL
```sql
DESCRIBE HISTORY t;  DESCRIBE DETAIL t;  DESCRIBE EXTENDED t;  SHOW TBLPROPERTIES t;
RESTORE TABLE t TO VERSION AS OF 3;
OPTIMIZE t;  OPTIMIZE t ZORDER BY (c);  ALTER TABLE t CLUSTER BY (c1, c2);  ALTER TABLE t CLUSTER BY AUTO;
VACUUM t;  VACUUM t RETAIN 168 HOURS;  VACUUM t DRY RUN;
ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS;
ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
ALTER TABLE t ADD CONSTRAINT c CHECK (x > 0);  ALTER TABLE t ALTER COLUMN id SET NOT NULL;
CREATE TABLE t2 SHALLOW CLONE t;  CREATE TABLE t3 DEEP CLONE t;
```

## Unity Catalog
```sql
CREATE CATALOG c;  CREATE SCHEMA c.s;  CREATE VOLUME c.s.v;
GRANT USE CATALOG ON CATALOG c TO `g`;  GRANT USE SCHEMA, SELECT ON SCHEMA c.s TO `g`;
SHOW GRANTS ON TABLE c.s.t;  ALTER TABLE t OWNER TO `g`;
ALTER TABLE t ALTER COLUMN email SET MASK mask_fn;   ALTER TABLE t SET ROW FILTER f ON (col);
SELECT current_user(), is_account_group_member('g');
SELECT * FROM c.information_schema.tables;
```

## dbutils
```python
dbutils.fs.ls(p); dbutils.fs.cp(a, b, recurse=True); dbutils.fs.rm(p, recurse=True); dbutils.fs.head(p)
dbutils.widgets.text("k", "default"); dbutils.widgets.get("k")
dbutils.secrets.get(scope="s", key="k")
dbutils.jobs.taskValues.set(key="k", value=1); dbutils.jobs.taskValues.get(taskKey="t", key="k")
dbutils.notebook.run("./nb", 600, {"arg": "x"}); dbutils.notebook.exit("result")
```

## Lakeflow Declarative Pipelines
```python
from pyspark import pipelines as dp          # legacy: import dlt
@dp.table                                     # streaming table (return readStream)
@dp.materialized_view                         # MV (return batch DF)
@dp.temporary_view
@dp.expect / expect_or_drop / expect_or_fail / expect_all_or_drop({...})
dp.create_streaming_table("t"); dp.create_auto_cdc_flow(target=, source=, keys=, sequence_by=, stored_as_scd_type=2)
```
```sql
CREATE OR REFRESH STREAMING TABLE b AS SELECT * FROM STREAM read_files('/Volumes/...', format => 'json');
CREATE OR REFRESH MATERIALIZED VIEW g AS SELECT ... FROM s;
CONSTRAINT x EXPECT (cond) ON VIOLATION DROP ROW | FAIL UPDATE
```

## CLI / Bundles
```bash
databricks auth login --host <url>
databricks bundle init | validate | deploy -t dev | run <job_key> -t dev | summary | destroy
databricks fs ls dbfs:/Volumes/...      databricks jobs list      databricks clusters list
```
