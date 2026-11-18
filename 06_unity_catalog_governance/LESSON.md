# Module 06 — Unity Catalog & data governance

## 1. Object model
```
Metastore (one per region)
 └─ Catalog            e.g. dev, prod, workspace          ← usually = environment or business domain
     └─ Schema         e.g. shopstream                    ← a.k.a. database
         ├─ Table      managed | external | streaming table | materialized view
         ├─ View
         ├─ Volume     managed | external  (non-tabular files: CSV, images, PDFs, JARs)
         ├─ Function   SQL/Python UDFs, row filters, column masks
         └─ Model      ML models
Securables outside the tree: storage credentials, external locations, connections (Lakehouse Federation), shares (Delta Sharing)
```
- **Managed table:** UC owns data and lifecycle. `DROP` deletes the data. Gets **predictive optimization** (auto OPTIMIZE/VACUUM). This is the default and recommended.
- **External table:** data lives at a path you manage (via an *external location*). `DROP` removes only metadata.
- **Volume:** governed folder for files. Path: `/Volumes/catalog/schema/volume/...`.

## 2. Privileges
To read `c.s.t` you need **`USE CATALOG` on c + `USE SCHEMA` on c.s + `SELECT` on t**. Privileges inherit downward.
```sql
GRANT USE CATALOG ON CATALOG prod TO `analysts`;
GRANT USE SCHEMA, SELECT ON SCHEMA prod.shopstream TO `analysts`;    -- all current + future tables
GRANT MODIFY ON TABLE prod.shopstream.silver_orders TO `etl-sp`;     -- INSERT/UPDATE/DELETE/MERGE
GRANT CREATE TABLE ON SCHEMA prod.shopstream TO `data-engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME prod.shopstream.landing TO `ingest-sp`;
REVOKE SELECT ON TABLE ... FROM `x`;   SHOW GRANTS ON TABLE ...;
ALTER TABLE t OWNER TO `data-engineers`;   -- owners have all privileges; prefer groups over users
```
Best practice: grant to **groups**, run production jobs as **service principals**, separate catalogs per environment (`dev`/`staging`/`prod`).

## 3. Fine-grained access control
- **Dynamic views**: `CASE WHEN is_account_group_member('pii_readers') THEN email ELSE '***' END`.
- **Column masks**: a SQL function attached to a column, applied on every query, for every user.
- **Row filters**: a boolean SQL function attached to a table that hides rows.
- **ABAC / tags**: tag columns (`ALTER TABLE ... ALTER COLUMN email SET TAGS ('pii'='email')`) and drive policies from tags at scale.

## 4. Other governance features
- **Lineage**: automatic table- and column-level lineage (Catalog Explorer → *Lineage* tab), from notebooks, jobs, pipelines and SQL.
- **System tables** (`system.access.audit`, `system.billing.usage`, `system.lakeflow.job_run_timeline`, `system.access.table_lineage`, …) are queryable logs for audit and cost.
- **Delta Sharing**: share tables with other organizations or workspaces without copying data.
- **Lakehouse Federation**: query PostgreSQL, MySQL, Snowflake, etc. in place via *connections* and *foreign catalogs*.
- **Information schema**: `SELECT * FROM workspace.information_schema.tables`.

## 5. Project 06: secure the ShopStream lakehouse
Notebook: [`06_governance.py`](06_governance.py)
1. Inspect the namespace, managed vs external, and `information_schema`.
2. Tag PII columns and add comments (documentation is governance too).
3. **Column mask**: `email` and `phone` are hidden unless you're in the `pii_readers` group.
4. **Row filter**: regional managers only see their country's customers.
5. A **dynamic view** for analysts as an alternative approach.
6. Grants for a realistic role model (data engineers, analysts, ETL service principal).
7. Query **lineage** and **audit** system tables.

> Free Edition note: you may be the only user, and some system tables may not be enabled. The grants still run
> (create the groups in *Settings → Identity and access* first, or the GRANTs fail with "principal not found").
> The masks and filters work fully: test them by adding yourself to, and removing yourself from, the groups.
