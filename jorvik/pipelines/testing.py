import pytest

import delta
from pyspark.testing import assertSchemaEqual
from pyspark.sql import SparkSession

from jorvik.pipelines.etl import ETL


@pytest.fixture
def spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.ui.enabled", "false")
    )

    return delta.configure_spark_with_delta_pip(builder).getOrCreate()


def smoke_test_etl(etl: ETL):
    """ Run a smoke test on the ETL pipeline. """
    spark = SparkSession.getActiveSession()
    data = tuple(spark.createDataFrame([], i.schema) for i in etl.inputs)

    transformed = etl.transform_func(*data)
    if not isinstance(transformed, tuple):
        transformed = (transformed,)

    for df, out in zip(transformed, etl.outputs):
        assertSchemaEqual(df.schema, out.schema)
