-- Lakeflow Declarative Pipeline: ShopStream Gold layer (SQL).
-- Lives in the same pipeline as shopstream_pipeline.py, so it can reference its tables by name.

CREATE OR REFRESH MATERIALIZED VIEW silver_orders_enriched
COMMENT 'Orders joined with products and current customer attributes.'
AS SELECT
  o.order_id, o.customer_id, o.product_id,
  coalesce(p.category, 'unknown') AS category, p.name AS product_name,
  o.quantity, o.unit_price, CAST(o.quantity * o.unit_price AS DECIMAL(12,2)) AS amount,
  o.status, o.channel, o.order_ts, to_date(o.order_ts) AS order_date,
  c.country, c.loyalty_tier
FROM silver_orders o
LEFT JOIN silver_products p USING (product_id)
LEFT JOIN dim_customer c ON o.customer_id = c.customer_id AND c.__END_AT IS NULL;

CREATE OR REFRESH MATERIALIZED VIEW gold_daily_revenue (
  CONSTRAINT revenue_not_negative EXPECT (revenue >= 0) ON VIOLATION FAIL UPDATE
)
COMMENT 'Revenue by day and category (cancelled/returned excluded).'
AS SELECT order_date, category,
          count(DISTINCT order_id) AS orders, sum(quantity) AS units, sum(amount) AS revenue
FROM silver_orders_enriched
WHERE status IN ('PLACED', 'SHIPPED', 'DELIVERED')
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_revenue_by_country
COMMENT 'Revenue by customer country and loyalty tier.'
AS SELECT coalesce(country, 'unknown') AS country, loyalty_tier,
          count(DISTINCT customer_id) AS customers, sum(amount) AS revenue
FROM silver_orders_enriched
WHERE status IN ('PLACED', 'SHIPPED', 'DELIVERED')
GROUP BY ALL;

CREATE OR REFRESH MATERIALIZED VIEW gold_return_rate
COMMENT 'Share of orders cancelled or returned, by category.'
AS SELECT category,
          count(*) AS orders,
          round(avg(CASE WHEN status IN ('CANCELLED', 'RETURNED') THEN 1 ELSE 0 END) * 100, 2) AS bad_outcome_pct
FROM silver_orders_enriched
GROUP BY ALL;
