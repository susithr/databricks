# Module 02 — Delta Lake deep dive

## 1. What Delta Lake is
A Delta table = **Parquet data files** + a **transaction log** (`_delta_log/` folder of JSON commits and periodic Parquet checkpoints).
The log is the source of truth: a file is only "in" the table if a committed log entry says so. That gives you:

| Feature | How | Why you care |
|---|---|---|
| **ACID transactions** | Optimistic concurrency on the log | No half-written tables, safe concurrent writers |
| **Time travel** | Old log versions point at old files | Audit, debugging, rollback (`RESTORE`) |
| **Schema enforcement** | Writes must match the table schema | Bad data is rejected, not silently stored |
| **Schema evolution** | `mergeSchema` / `ALTER TABLE ADD COLUMN` | Add columns safely |
| **MERGE / UPDATE / DELETE** | Rewrite affected files (or **deletion vectors**) | Upserts and CDC on a data lake |
| **Data skipping** | min/max stats per file in the log | Queries skip irrelevant files |
| **Change Data Feed (CDF)** | Row-level change log | Incremental downstream processing |

## 2. Commands you must know
```sql
CREATE TABLE t (id INT, name STRING) ;                         -- managed Delta table (Delta is the default)
CREATE OR REPLACE TABLE t AS SELECT ...;                       -- CTAS
INSERT INTO t VALUES (...);  INSERT OVERWRITE t SELECT ...;
UPDATE t SET x = 1 WHERE id = 5;   DELETE FROM t WHERE ...;

MERGE INTO target t USING source s ON t.id = s.id
  WHEN MATCHED AND s.op = 'DELETE' THEN DELETE
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *
  WHEN NOT MATCHED BY SOURCE THEN DELETE;                      -- optional: delete rows missing from source

DESCRIBE HISTORY t;   DESCRIBE DETAIL t;   DESCRIBE EXTENDED t;
SELECT * FROM t VERSION AS OF 3;   SELECT * FROM t TIMESTAMP AS OF '2026-01-01';
RESTORE TABLE t TO VERSION AS OF 3;

OPTIMIZE t;                         -- compact small files
OPTIMIZE t ZORDER BY (col);         -- legacy co-location; prefer liquid clustering:
ALTER TABLE t CLUSTER BY (col);     -- liquid clustering (Module 08)
VACUUM t RETAIN 168 HOURS;          -- physically delete unreferenced files older than 7 days (breaks time travel past that)

ALTER TABLE t SET TBLPROPERTIES (delta.enableChangeDataFeed = true);
SELECT * FROM table_changes('t', 2);  -- _change_type: insert / update_preimage / update_postimage / delete

ALTER TABLE t ADD CONSTRAINT qty_pos CHECK (quantity > 0);   -- enforced on every write
CREATE TABLE t (id BIGINT GENERATED ALWAYS AS IDENTITY, ...);  -- surrogate keys

CREATE TABLE t2 SHALLOW CLONE t;   -- copies metadata only (great for testing)
CREATE TABLE t3 DEEP CLONE t;      -- copies data too (backup, migration)
```
Python equivalents live in `delta.tables.DeltaTable` (`forName`, `.merge()`, `.history()`, `.restoreToVersion()`).

## 3. Interview-grade details
- **Small-file problem:** many tiny files make reads slow. Fix with `OPTIMIZE`, **auto-optimize / optimized writes**, or predictive optimization (UC managed tables).
- **VACUUM default retention** is 7 days. Shorter retention can break concurrent readers and time travel.
- **Deletion vectors**: UPDATE/DELETE/MERGE mark rows deleted instead of rewriting whole files (faster writes; rewritten later by OPTIMIZE).
- **MERGE fails** if multiple source rows match the same target row. **Deduplicate the source first.**
- **Concurrency conflicts** (`ConcurrentAppendException`, etc.) happen when two writers touch the same files. Partition/cluster so they don't overlap, or retry.

## 4. Project 02: customer master table
Notebook: [`02_customer_master.py`](02_customer_master.py). Using the customer CDC files from the landing volume, you will:
1. Build `customers` from day 1 (all INSERTs).
2. Apply day 2's changes with **MERGE**: inserts, updates and deletes in one atomic statement.
3. Break something on purpose (a bad overwrite), then **time travel** and **RESTORE**.
4. Try **schema enforcement** (a write gets rejected) and then **schema evolution** (add a column).
5. Turn on **Change Data Feed** and read exactly which rows changed.
6. Add a **CHECK constraint**, run `OPTIMIZE` and `VACUUM`, and inspect `_delta_log`.
