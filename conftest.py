import os
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip


@pytest.fixture(scope="session")
def spark():
    os.environ["_PYSPARK_DRIVER_CONN_MEM_LIMIT"] = "2g"
    
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("nordpool-tests")
        .config("spark.sql.warehouse.dir", "memory:/")
        .config("spark.sql.catalogImplementation", "in-memory")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.ui.enabled", "false")
    )
    
    # Configure with delta
    builder = configure_spark_with_delta_pip(builder)
    
    # Explicitly set Delta configs (in case configure_spark_with_delta_pip didn't apply them)
    builder = (
        builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    
    session = builder.getOrCreate()
    yield session
    session.stop()