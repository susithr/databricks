"""Tests for the code that ships inside the bundle (src/shopstream)."""
import filecmp
import os

from shopstream.transforms import clean_orders, split_valid

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(os.path.dirname(HERE))


def test_bundle_copy_matches_course_code():
    """src/shopstream is a copy of common/ so the bundle is self-contained. Keep them in sync."""
    for f in ["transforms.py", "datagen.py"]:
        assert filecmp.cmp(os.path.join(ROOT, "common", f),
                           os.path.join(ROOT, "07_jobs_and_bundles", "src", "shopstream", f), shallow=False), f


def test_quarantine_rate_on_generated_day(spark):
    from shopstream.datagen import ShopStreamGenerator
    from transforms import BRONZE_ORDER_COLUMNS
    rows = ShopStreamGenerator(seed=42).order_rows(1)
    df = spark.createDataFrame([{c: None if r.get(c) is None else str(r[c]) for c in BRONZE_ORDER_COLUMNS} for r in rows],
                               ", ".join(f"{c} STRING" for c in BRONZE_ORDER_COLUMNS))
    valid, bad = split_valid(clean_orders(df))
    pct = 100 * bad.count() / df.count()
    assert pct < 10, "default DQ gate threshold would fail on normal data"
