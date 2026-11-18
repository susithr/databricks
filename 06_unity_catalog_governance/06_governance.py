# Databricks notebook source
# MAGIC %md
# MAGIC # Project 06: secure the ShopStream lakehouse with Unity Catalog
# MAGIC Prerequisite: Module 02 (the `customers` table) and Module 03 (gold tables).

# COMMAND ----------

# MAGIC %run ../common/config

# COMMAND ----------

# MAGIC %md ## 1. Explore the namespace

# COMMAND ----------

display(spark.sql(f"""
SELECT table_name, table_type, data_source_format, created_by, comment
FROM {CATALOG}.information_schema.tables
WHERE table_schema = '{SCHEMA}'
ORDER BY table_name"""))

# COMMAND ----------

display(spark.sql(f"DESCRIBE EXTENDED {t('customers')}"))   # look for Type: MANAGED and Location

# COMMAND ----------

print("me:", spark.sql("SELECT current_user()").first()[0])
display(spark.sql("SELECT is_account_group_member('pii_readers') AS in_pii_readers, "
                  "is_account_group_member('admins') AS in_admins"))

# COMMAND ----------

# MAGIC %md ## 2. Document and tag
# MAGIC Comments show up in Catalog Explorer and help AI/BI Genie understand your data. Tags drive discovery and policies.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE customers IS 'Customer master (SCD1). Source: CRM CDC feed. Owner: data-engineering.';
# MAGIC ALTER TABLE customers ALTER COLUMN email COMMENT 'Customer email. PII.';
# MAGIC ALTER TABLE customers ALTER COLUMN email SET TAGS ('pii' = 'email');
# MAGIC ALTER TABLE customers ALTER COLUMN phone SET TAGS ('pii' = 'phone');
# MAGIC ALTER TABLE customers SET TAGS ('domain' = 'sales', 'tier' = 'silver');

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT table_name, column_name, tag_name, tag_value
# MAGIC FROM workspace.information_schema.column_tags WHERE schema_name = 'shopstream'

# COMMAND ----------

# MAGIC %md ## 3. Column masks for PII
# MAGIC A mask is a SQL UDF. It receives the column value and returns what the current user is allowed to see.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_email(email STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('pii_readers') THEN email
# MAGIC             ELSE concat(left(email, 1), '***@', split_part(email, '@', 2)) END;
# MAGIC
# MAGIC CREATE OR REPLACE FUNCTION mask_phone(phone STRING) RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('pii_readers') THEN phone
# MAGIC             ELSE concat('***-', right(phone, 4)) END;
# MAGIC
# MAGIC ALTER TABLE customers ALTER COLUMN email SET MASK mask_email;
# MAGIC ALTER TABLE customers ALTER COLUMN phone SET MASK mask_phone;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT customer_id, first_name, email, phone, country FROM customers LIMIT 5   -- masked unless you're in pii_readers

# COMMAND ----------

# MAGIC %md ## 4. Row filter: regional managers see only their region
# MAGIC Mapping table: which group may see which country. Admins see everything.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE region_access (group_name STRING, country STRING);
# MAGIC INSERT INTO region_access VALUES
# MAGIC   ('mgr_us', 'US'), ('mgr_us', 'CA'),
# MAGIC   ('mgr_eu', 'GB'), ('mgr_eu', 'DE'), ('mgr_eu', 'FR'),
# MAGIC   ('mgr_apac', 'IN');
# MAGIC
# MAGIC -- careful: name the parameter differently from the column, or `country` inside the
# MAGIC -- subquery would resolve to r.country and the filter would always be true
# MAGIC CREATE OR REPLACE FUNCTION country_filter(p_country STRING) RETURNS BOOLEAN
# MAGIC RETURN is_account_group_member('admins')
# MAGIC     OR EXISTS (SELECT 1 FROM workspace.shopstream.region_access r
# MAGIC                WHERE r.country = p_country AND is_account_group_member(r.group_name));
# MAGIC
# MAGIC ALTER TABLE customers SET ROW FILTER country_filter ON (country);

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT country, count(*) FROM customers GROUP BY country   -- try adding yourself to mgr_eu and removing yourself from admins

# COMMAND ----------

# MAGIC %md ## 5. Alternative: a dynamic view
# MAGIC Before masks and filters existed, this was the pattern. Still useful for sharing a curated subset.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW v_customers_analyst AS
# MAGIC SELECT customer_id, country, loyalty_tier,
# MAGIC        sha2(email, 256) AS email_hash      -- joinable but not readable
# MAGIC FROM customers;
# MAGIC SELECT * FROM v_customers_analyst LIMIT 5;

# COMMAND ----------

# MAGIC %md ## 6. Role model with grants
# MAGIC Create the groups `data_engineers`, `analysts`, `pii_readers`, `mgr_eu` in *Settings → Identity and access* first.

# COMMAND ----------

grants = f"""
GRANT USE CATALOG ON CATALOG {CATALOG} TO `data_engineers`;
GRANT USE SCHEMA, CREATE TABLE, CREATE FUNCTION, CREATE VOLUME, SELECT, MODIFY ON SCHEMA {CATALOG}.{SCHEMA} TO `data_engineers`;
GRANT READ VOLUME, WRITE VOLUME ON VOLUME {CATALOG}.{SCHEMA}.{VOLUME} TO `data_engineers`;

GRANT USE CATALOG ON CATALOG {CATALOG} TO `analysts`;
GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO `analysts`;
GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.gold_daily_revenue TO `analysts`;
GRANT SELECT ON TABLE {CATALOG}.{SCHEMA}.gold_customer_ltv TO `analysts`;
GRANT SELECT ON VIEW {CATALOG}.{SCHEMA}.v_customers_analyst TO `analysts`;
"""
for stmt in [s.strip() for s in grants.split(";") if s.strip()]:
    try:
        spark.sql(stmt); print("OK  ", stmt[:90])
    except Exception as e:
        print("SKIP", stmt[:90], "->", str(e).splitlines()[0][:80])

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS ON SCHEMA {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md ## 7. Lineage and audit from system tables
# MAGIC (Requires system tables to be enabled. You can also open any table in Catalog Explorer → **Lineage**.)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT source_table_full_name, target_table_full_name, entity_type, max(event_time) AS last_seen
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE target_table_schema = 'shopstream'
# MAGIC GROUP BY ALL ORDER BY last_seen DESC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT event_time, user_identity.email, action_name, request_params.full_name_arg AS object
# MAGIC FROM system.access.audit
# MAGIC WHERE service_name = 'unityCatalog' AND event_date >= current_date() - 1
# MAGIC   AND request_params.full_name_arg LIKE '%shopstream%'
# MAGIC ORDER BY event_time DESC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## Your turn
# MAGIC 1. Add a mask on `gold_customer_ltv.lifetime_value` that rounds to the nearest 100 for non-`finance` users.
# MAGIC 2. Query `system.billing.usage` to find what this course has cost you in DBUs this week.
# MAGIC 3. Explain to a colleague why masks and filters are safer than dynamic views. (Hint: they protect the *table itself*, so every access path is covered.)
# MAGIC 4. Clean up when you're done: `ALTER TABLE customers DROP ROW FILTER; ALTER TABLE customers ALTER COLUMN email DROP MASK;`
# MAGIC    (Later modules expect unmasked data.)
# MAGIC
# MAGIC ---
# MAGIC ## SOLUTION

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE FUNCTION mask_ltv(v DECIMAL(22,2)) RETURNS DECIMAL(22,2)
# MAGIC RETURN CASE WHEN is_account_group_member('finance') THEN v ELSE round(v, -2) END;
# MAGIC -- ALTER TABLE gold_customer_ltv ALTER COLUMN lifetime_value SET MASK mask_ltv;
# MAGIC
# MAGIC -- SELECT usage_date, sku_name, sum(usage_quantity) AS dbus FROM system.billing.usage
# MAGIC -- WHERE usage_date >= current_date() - 7 GROUP BY ALL ORDER BY usage_date;
