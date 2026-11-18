# Module 00 — Databricks platform tour & setup

## 1. What Databricks is (the 60-second version)
Databricks is a **lakehouse platform**: you keep data as open files (Delta Lake / Parquet) in cheap cloud
object storage, and get warehouse features on top: ACID transactions, SQL, governance, BI.
A data engineer on Databricks mostly does three things:
1. **Ingest** raw data (files, databases, Kafka, APIs) into Delta tables.
2. **Transform** it with Spark (Python/SQL) through Bronze → Silver → Gold layers.
3. **Operate** it: schedule (Jobs), govern (Unity Catalog), test and deploy (Asset Bundles + CI/CD), monitor and tune.

## 2. Architecture you must be able to explain in interviews
| Concept | Meaning |
|---|---|
| **Control plane** | Databricks-managed: web UI, notebooks, job scheduler, Unity Catalog metastore. |
| **Compute plane** | Where Spark actually runs. **Serverless** (Databricks-managed, starts in seconds) or **classic** (VMs in your cloud account). |
| **Storage** | Your cloud object storage (S3 / ADLS / GCS). Tables are Delta files there. |
| **Workspace** | Your environment: notebooks, Git folders, jobs, SQL warehouses. |
| **Metastore → Catalog → Schema → Table/View/Volume/Function** | Unity Catalog's namespace: `catalog.schema.table`. |

### Compute types
| Compute | Use it for |
|---|---|
| **All-purpose cluster** | Interactive development in notebooks (classic). |
| **Job cluster** | Created for one job run, then terminated: cheaper for production. |
| **Serverless (notebooks/jobs/pipelines)** | No cluster management. The default in Free Edition. |
| **SQL Warehouse** | SQL queries, dashboards, BI tools (Power BI, Tableau). |

Cluster vocabulary: **driver** (plans the work) and **workers/executors** (do the work),
**Databricks Runtime (DBR)** version, **autoscaling**, **auto-termination**, **Photon** (vectorized C++ engine).

### Serverless gotchas (Free Edition)
- No RDD API, no `sc`/`spark.sparkContext`, no `df.cache()`/`persist()` on serverless. Use DataFrames.
- Use **Volumes** (`/Volumes/catalog/schema/volume/...`) for files. DBFS root is legacy.
- Only Python and SQL notebooks (Scala/R need classic clusters).

## 3. Notebook skills
- Magic commands: `%sql`, `%python`, `%md`, `%run ./other_notebook`, `%pip install x`, `%sh`.
- `display(df)` gives an interactive table and chart. `dbutils.fs.ls(path)` lists files.
- `dbutils.widgets.text("env", "dev")` / `dbutils.widgets.get("env")` adds notebook parameters (used by Jobs).
- `dbutils.secrets.get(scope, key)` reads secrets. Never hard-code credentials.
- Notebooks saved as `.py` with `# COMMAND ----------` separators are plain files that work with Git.

## 4. Project 00: set up your lakehouse
Run [`00_setup_workspace.py`](00_setup_workspace.py). It:
1. creates schema `workspace.shopstream` and volume `landing`,
2. writes the product catalog and **day 1** of orders and customers into the volume,
3. lists the files so you can see what "raw data" looks like.

Then open **Catalog Explorer** (left sidebar) and find the schema and volume you created.

### Your turn
1. In Catalog Explorer, click the volume and preview `orders_001.json`. Which "messy" records can you spot?
2. Create a SQL Warehouse (or use the Serverless Starter Warehouse) and run
   `SELECT * FROM read_files('/Volumes/workspace/shopstream/landing/orders') LIMIT 10` in the SQL editor.
3. Explain in your own words: what's the difference between a **managed table**, an **external table** and a **volume**?
   (Answer in Module 06.)
