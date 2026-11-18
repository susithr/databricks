"""Reusable, unit-testable ShopStream transformations.

Rule of thumb used by real teams: notebooks do I/O (read/write tables),
plain functions like these do the logic (DataFrame in -> DataFrame out),
so they can be tested with pytest on a laptop or in CI.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

# Bronze keeps everything as raw strings, so nothing is lost when upstream sends junk.
BRONZE_ORDER_COLUMNS = [
    "order_id", "customer_id", "product_id", "quantity", "unit_price",
    "status", "order_ts", "updated_ts", "channel",
]


def parse_timestamp(col: str):
    """Accept ISO-8601 and dd/MM/yyyy HH:mm:ss (one upstream app sends the latter)."""
    return F.coalesce(
        F.try_to_timestamp(F.col(col)),
        F.try_to_timestamp(F.col(col), F.lit("dd/MM/yyyy HH:mm:ss")),
    )


def clean_orders(bronze: DataFrame) -> DataFrame:
    """Typed, standardized orders plus a `dq_errors` array describing what's wrong (empty = valid)."""
    typed = (
        bronze
        .withColumn("order_id", F.trim("order_id"))
        .withColumn("customer_id", F.expr("try_cast(customer_id AS INT)"))
        .withColumn("product_id", F.upper(F.trim("product_id")))
        .withColumn("quantity", F.expr("try_cast(quantity AS INT)"))
        .withColumn("unit_price", F.expr("try_cast(unit_price AS DECIMAL(10,2))"))
        .withColumn("status", F.upper(F.trim("status")))
        .withColumn("order_ts", parse_timestamp("order_ts"))
        .withColumn("updated_ts", F.coalesce(parse_timestamp("updated_ts"), F.col("order_ts")))
        .withColumn("channel", F.lower(F.trim("channel")))
    )
    checks = [
        (F.col("order_id").isNull(), "missing_order_id"),
        (F.col("customer_id").isNull(), "missing_customer_id"),
        (F.col("quantity").isNull() | (F.col("quantity") <= 0), "invalid_quantity"),
        (F.col("unit_price").isNull() | (F.col("unit_price") < 0), "invalid_price"),
        (F.col("order_ts").isNull(), "invalid_order_ts"),
    ]
    errors = F.filter(F.array(*[F.when(c, F.lit(name)) for c, name in checks]), lambda x: x.isNotNull())
    return typed.withColumn("dq_errors", errors)


def split_valid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """(valid rows, quarantined rows). Never silently drop bad data: quarantine it."""
    return (
        df.filter(F.size("dq_errors") == 0).drop("dq_errors"),
        df.filter(F.size("dq_errors") > 0),
    )


def latest_per_key(df: DataFrame, key: str | list[str], order_col: str) -> DataFrame:
    """Keep the most recent row per key (dedupes exact duplicates and late updates)."""
    keys = [key] if isinstance(key, str) else key
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def enrich_orders(orders: DataFrame, products: DataFrame) -> DataFrame:
    """Join product info; unknown products are kept with category 'unknown'."""
    p = products.select("product_id", F.col("name").alias("product_name"), "category")
    return (
        orders.join(F.broadcast(p), "product_id", "left")
        .withColumn("category", F.coalesce("category", F.lit("unknown")))
        .withColumn("amount", (F.col("quantity") * F.col("unit_price")).cast("decimal(12,2)"))
        .withColumn("order_date", F.to_date("order_ts"))
    )


REVENUE_STATUSES = ["PLACED", "SHIPPED", "DELIVERED"]


def daily_revenue(silver_orders: DataFrame) -> DataFrame:
    return (
        silver_orders.filter(F.col("status").isin(REVENUE_STATUSES))
        .groupBy("order_date", "category")
        .agg(
            F.countDistinct("order_id").alias("orders"),
            F.sum("quantity").alias("units"),
            F.sum("amount").alias("revenue"),
        )
    )


def customer_ltv(silver_orders: DataFrame) -> DataFrame:
    valid = silver_orders.filter(F.col("status").isin(REVENUE_STATUSES))
    return valid.groupBy("customer_id").agg(
        F.countDistinct("order_id").alias("orders"),
        F.sum("amount").alias("lifetime_value"),
        F.min("order_ts").alias("first_order_ts"),
        F.max("order_ts").alias("last_order_ts"),
    )
