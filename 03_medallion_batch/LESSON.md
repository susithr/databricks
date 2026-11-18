# Module 03 — Medallion architecture (batch pipelines)

## 1. The three layers
| Layer | Contents | Rules |
|---|---|---|
| **Bronze** | Raw data exactly as received, plus ingestion metadata (`_ingested_at`, `_source_file`) | Append-only. Keep everything, even junk (as strings or in `_rescued_data`). Lets you **replay** history. |
| **Silver** | Cleaned, typed, deduplicated, conformed entities (orders, customers) | Enforce data quality, quarantine bad rows, MERGE late updates. One row per business key. |
| **Gold** | Business-level aggregates and models (star schema, KPIs, features) | Shaped for consumers: BI, ML, APIs. |

## 2. Design principles (these come up in interviews)
- **Idempotency:** running a job twice must give the same result. Use MERGE on keys, `replaceWhere` for partitions, or checkpoints (streaming). Avoid blind `append` in Silver and Gold.
- **Never lose data:** don't `filter` away bad rows. Send them to a **quarantine** table with the reason.
- **Separate logic from I/O:** transformations as pure functions (see `common/transforms.py`) are testable.
- **Incremental over full refresh:** process only new data (Module 04). A full rebuild is fine for small Gold tables.
- **Metadata columns:** `_source_file` (`_metadata.file_path`), `_ingested_at`, batch ids. You'll need them when debugging.
- **Late-arriving data:** an order's status changes days later. Silver must be updatable (MERGE with a recency check).

## 3. Useful techniques
```python
spark.read.format("json").load(path).select("*", "_metadata.file_path")  # file metadata column
df.write.mode("overwrite").option("replaceWhere", "order_date = '2026-01-02'").saveAsTable(t)  # overwrite one slice
F.try_to_timestamp / F.try_cast   # don't fail the job on bad values; get NULLs instead
```
`COPY INTO` is SQL's idempotent batch file loader (it remembers which files it already loaded):
```sql
COPY INTO bronze_orders FROM '/Volumes/.../orders' FILEFORMAT = JSON
FORMAT_OPTIONS ('inferSchema' = 'false') COPY_OPTIONS ('mergeSchema' = 'true');
```

## 4. Project 03: ShopStream batch pipeline
Notebook: [`03_medallion_pipeline.py`](03_medallion_pipeline.py)

```
landing/orders/*.json ──COPY INTO──► bronze_orders ──clean_orders()──► silver_orders  ──► gold_daily_revenue
landing/products      ─────────────► silver_products ─┘     └─bad rows─► quarantine_orders  └► gold_customer_ltv
```
You'll load day 1, then land day 2 (which includes duplicates and late status updates) and rerun the pipeline.
You should see Silver change via MERGE, not grow with duplicates.

### Your turn
- Add a `gold_channel_mix` table: revenue share by channel per day.
- Add a data-quality report: % of quarantined rows per error type per day.
- Prove idempotency: run the whole notebook twice and assert all row counts are unchanged.
