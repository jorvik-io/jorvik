import pytest
from fixtures.pipelines import *  # noqa: F403 F401

from pyspark.errors import PySparkAssertionError

from jorvik.pipelines.testing import spark, SparkSession, smoke_test_etl  # noqa: F811 F401


def test_etl_smoke_test_success(spark: SparkSession, simple_join):  # noqa: F811
    """ Verify a correct etl generates expected schema. """
    smoke_test_etl(simple_join)


def test_etl_smoke_incorrect_schema(spark: SparkSession, incorrect_schema):  # noqa: F811
    """ Verify a wrong etl does not generate the expected schema. """
    with pytest.raises(PySparkAssertionError):
        smoke_test_etl(incorrect_schema)
