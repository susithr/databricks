# Databricks Data Engineering Bootcamp

A hands-on course where **each topic comes with a project you build in Databricks**.
Read the lesson (`LESSON.md`), then open the notebook in the same folder and run it cell by cell.
Every module adds to one shared **e-commerce lakehouse** ("ShopStream"). By the end you'll have built
a production-style platform you can show in interviews.

```
 raw JSON/CSV files ──► BRONZE (raw, append-only) ──► SILVER (clean, deduped, SCD2) ──► GOLD (business KPIs)
   (Volume)              Auto Loader / Streaming        Delta MERGE / expectations        Dashboards / SQL
                         ────────── governed by Unity Catalog, orchestrated by Lakeflow Jobs, deployed with Asset Bundles ──────────
```

## Roadmap (about 6–8 weeks at 1–2 hours a day)

| # | Module | You learn | Project you build |
|---|--------|-----------|-------------------|
| 00 | [Setup](00_setup/LESSON.md) | Workspace, compute, catalogs, volumes, notebooks | Create the bootcamp catalog/schema/volume and generate source data |
| 01 | [Spark fundamentals](01_spark_fundamentals/LESSON.md) | DataFrames, transformations vs actions, joins, windows, Spark SQL, lazy evaluation | **Taxi trip analytics** on `samples.nyctaxi.trips` |
| 02 | [Delta Lake](02_delta_lake/LESSON.md) | ACID, transaction log, MERGE, time travel, schema evolution, OPTIMIZE, VACUUM, Change Data Feed | **Customer master table** with upserts, rollback and audit |
| 03 | [Medallion architecture (batch)](03_medallion_batch/LESSON.md) | Bronze/Silver/Gold design, data quality, idempotent loads | **ShopStream batch pipeline**: orders → revenue KPIs |
| 04 | [Auto Loader & Structured Streaming](04_autoloader_streaming/LESSON.md) | `cloudFiles`, checkpoints, schema inference/rescue, triggers, `foreachBatch`, SCD Type 2 | **Incremental ingestion** + SCD2 customer dimension |
| 05 | [Lakeflow Declarative Pipelines (DLT)](05_lakeflow_pipelines/LESSON.md) | Streaming tables, materialized views, expectations, AUTO CDC | **Declarative ShopStream** pipeline with data-quality rules |
| 06 | [Unity Catalog & governance](06_unity_catalog_governance/LESSON.md) | 3-level namespace, grants, row filters, column masks, lineage, volumes | **Secure the lakehouse**: PII masking & regional row-level security |
| 07 | [Jobs, Asset Bundles & CI/CD](07_jobs_and_bundles/LESSON.md) | Lakeflow Jobs, task dependencies, parameters, DABs, pytest, GitHub Actions | **Deploy ShopStream as code** to dev/prod |
| 08 | [Performance tuning](08_performance_tuning/LESSON.md) | Partitioning, liquid clustering, file sizes, AQE, broadcast joins, skew, Spark UI | **Make a slow job fast** and measure it |
| 09 | [Capstone](09_capstone/LESSON.md) | Everything together | **Real-time IoT/clickstream lakehouse** (you build it, rubric included) |

Extras: [CHEATSHEET.md](CHEATSHEET.md) (commands you'll use daily) ·
[CERTIFICATION_AND_INTERVIEW.md](CERTIFICATION_AND_INTERVIEW.md) (Databricks Data Engineer Associate/Professional prep + interview questions).

## How to use this repo

1. **Get a free workspace:** sign up for **Databricks Free Edition** (search "Databricks Free Edition").
   It's serverless and has Unity Catalog, which is all this course needs. (A 14-day trial on AWS/Azure/GCP also works.)
2. **Get the code into Databricks:** in the workspace, go to *Workspace → Create → Git folder* and paste this
   repo's GitHub URL (push this folder to your own GitHub first), **or** zip the folder and use *Import*.
   Files starting with `# Databricks notebook source` open as notebooks.
3. Run [`00_setup/00_setup_workspace.py`](00_setup/00_setup_workspace.py) once. Every other notebook
   reads its settings from [`common/config.py`](common/config.py) via `%run ../common/config`.
4. For each module: **read the LESSON → run the notebook → do the "Your turn" exercises at the end.**
   Solutions are hidden behind a `SOLUTION` heading at the bottom of each notebook. Try first.

## Running parts locally (optional)

The pure-Python logic (data generator, transformations) is unit-tested with PySpark + Delta on your laptop:

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -q
```

This mirrors what real teams do: write logic as functions, test them locally/CI, and run them on Databricks.

## Repo layout

```
common/                    shared config + synthetic data generator (used by all modules)
00_setup … 09_capstone/    one folder per module: LESSON.md + project notebook(s)
07_jobs_and_bundles/       a full Databricks Asset Bundle project (databricks.yml, src/, tests/)
tests/                     local pytest suite for the shared transformations
```
