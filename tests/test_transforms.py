from decimal import Decimal

from datagen import ShopStreamGenerator
from transforms import (BRONZE_ORDER_COLUMNS, clean_orders, customer_ltv, daily_revenue,
                        enrich_orders, latest_per_key, split_valid)


def bronze(spark, rows):
    """Bronze = everything as strings, like COPY INTO with primitivesAsString."""
    as_str = [{c: (None if r.get(c) is None else str(r[c])) for c in BRONZE_ORDER_COLUMNS} for r in rows]
    return spark.createDataFrame(as_str, ", ".join(f"{c} STRING" for c in BRONZE_ORDER_COLUMNS))


def row(**kw):
    base = dict(order_id="O1", customer_id=1, product_id="p0001", quantity=2, unit_price=10.5,
                status="placed", order_ts="2026-01-01T10:00:00", updated_ts=None, channel=" Web ")
    base.update(kw)
    return base


def test_clean_orders_types_and_standardizes(spark):
    out = clean_orders(bronze(spark, [row()])).first()
    assert out.product_id == "P0001" and out.status == "PLACED" and out.channel == "web"
    assert out.quantity == 2 and out.unit_price == Decimal("10.50")
    assert out.updated_ts == out.order_ts          # defaults to order_ts
    assert out.dq_errors == []


def test_clean_orders_flags_bad_rows(spark):
    df = clean_orders(bronze(spark, [
        row(order_id="A", quantity=-1),
        row(order_id="B", customer_id=None),
        row(order_id="C", order_ts="not a date"),
        row(order_id="D", order_ts="02/01/2026 13:45:00"),   # alt format is VALID
    ]))
    errs = {r.order_id: r.dq_errors for r in df.collect()}
    assert errs == {"A": ["invalid_quantity"], "B": ["missing_customer_id"],
                    "C": ["invalid_order_ts"], "D": []}
    valid, bad = split_valid(df)
    assert valid.count() == 1 and bad.count() == 3


def test_latest_per_key_keeps_newest_update(spark):
    df = clean_orders(bronze(spark, [
        row(status="PLACED", updated_ts="2026-01-01T10:00:00"),
        row(status="PLACED", updated_ts="2026-01-01T10:00:00"),     # exact duplicate
        row(status="SHIPPED", updated_ts="2026-01-02T09:00:00"),    # late update
    ]))
    out = latest_per_key(df, "order_id", "updated_ts").collect()
    assert len(out) == 1 and out[0].status == "SHIPPED"


def test_gold_tables(spark):
    products = spark.createDataFrame([("P0001", "Lamp", "home")], "product_id STRING, name STRING, category STRING")
    orders = clean_orders(bronze(spark, [
        row(order_id="O1", quantity=2, unit_price=10),
        row(order_id="O2", quantity=1, unit_price=5, product_id="P9999"),
        row(order_id="O3", quantity=1, unit_price=99, status="CANCELLED"),
    ]))
    silver = enrich_orders(orders.drop("dq_errors"), products)
    rev = {r.category: r.revenue for r in daily_revenue(silver).collect()}
    assert rev == {"home": Decimal("20.00"), "unknown": Decimal("5.00")}   # cancelled excluded
    ltv = customer_ltv(silver).first()
    assert ltv.orders == 2 and ltv.lifetime_value == Decimal("25.00")


def test_generated_data_flows_end_to_end(spark):
    gen = ShopStreamGenerator(seed=1)
    rows = gen.order_rows(1, n=300) + gen.order_rows(2, n=300)
    valid, bad = split_valid(clean_orders(bronze(spark, rows)))
    silver = latest_per_key(valid, "order_id", "updated_ts")
    assert 0 < bad.count() < 0.1 * len(rows)                                   # some junk, not too much
    assert silver.count() == silver.select("order_id").distinct().count()     # unique keys
    assert silver.filter("status != 'PLACED'").count() > 0                    # late updates applied


def test_datagen_is_deterministic():
    assert ShopStreamGenerator(seed=7).order_rows(3) == ShopStreamGenerator(seed=7).order_rows(3)
    assert ShopStreamGenerator(seed=7).customer_rows(2) == ShopStreamGenerator(seed=7).customer_rows(2)
