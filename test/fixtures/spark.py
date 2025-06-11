from pyspark.sql import SparkSession
import pytest


@pytest.fixture
def spark() -> SparkSession:
    return SparkSession.builder.getOrCreate()
