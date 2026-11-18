import os
import sys

import pytest

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(ROOT, "common"))
sys.path.insert(0, os.path.join(ROOT, "07_jobs_and_bundles", "src"))


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession with Delta Lake, the same APIs you use on Databricks."""
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("shopstream-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()
