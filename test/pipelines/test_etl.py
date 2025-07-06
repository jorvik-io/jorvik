import pytest
from fixtures.pipelines import *  # noqa: F403 F401

from pyspark.testing import assertDataFrameEqual

from jorvik import storage
from jorvik.pipelines.testing import spark, SparkSession  # noqa: F401


def test_etl_runs_success(spark: SparkSession, simple_join):  # noqa: F811
    """ Verify a correct etl generates expected schema. """

    first = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": "1.0"},
        {"id": 2, "name": "Bob", "value": "2.0"},
        {"id": 3, "name": "Cathy", "value": "3.0"},
    ])

    second = spark.createDataFrame([
        {"id": 1, "another_name": "Another Alice", "another_value": "1.0"},
        {"id": 2, "another_name": "Another Bob", "another_value": "2.0"},
        {"id": 3, "another_name": "Another Cathy", "another_value": "3.0"},
    ])

    st = storage.configure()

    st.write(first, "/tmp/simple_join/first", format="delta", mode="overwrite")
    st.write(second, "/tmp/simple_join/second", format="delta", mode="overwrite")

    simple_join.run()

    result = st.read("/tmp/simple_join/out", format="delta")
    expected = first.join(second, on=["id"], how="inner")

    assertDataFrameEqual(result, expected)


def test_etl_without_schemas_runs_success(spark: SparkSession, simple_join_without_schemas):  # noqa: F811
    """ Verify a correct etl generates expected schema. """

    first = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": "1.0"},
        {"id": 2, "name": "Bob", "value": "2.0"},
        {"id": 3, "name": "Cathy", "value": "3.0"},
    ])

    second = spark.createDataFrame([
        {"id": 1, "another_name": "Another Alice", "another_value": "1.0"},
        {"id": 2, "another_name": "Another Bob", "another_value": "2.0"},
        {"id": 3, "another_name": "Another Cathy", "another_value": "3.0"},
    ])

    st = storage.configure()

    st.write(first, "/tmp/simple_join/first", format="delta", mode="overwrite")
    st.write(second, "/tmp/simple_join/second", format="delta", mode="overwrite")

    simple_join_without_schemas.run()

    result = st.read("/tmp/simple_join/out", format="delta")
    expected = first.join(second, on=["id"], how="inner")

    assertDataFrameEqual(result, expected)

def test_etl_with_incorrect_schemas_fail(spark: SparkSession, incorrect_schema):  # noqa: F811
    """ Verify ETL with wrong schema fails instead of generating incorrect schema. """
    first = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": "1.0"},
        {"id": 2, "name": "Bob", "value": "2.0"},
        {"id": 3, "name": "Cathy", "value": "3.0"},
    ])

    second = spark.createDataFrame([
        {"id": 1, "another_name": "Another Alice", "another_value": "1.0"},
        {"id": 2, "another_name": "Another Bob", "another_value": "2.0"},
        {"id": 3, "another_name": "Another Cathy", "another_value": "3.0"},
    ])

    st = storage.configure()

    st.write(first, "/tmp/simple_join/first", format="delta", mode="overwrite")
    st.write(second, "/tmp/simple_join/second", format="delta", mode="overwrite")

    with pytest.raises(AssertionError):
        incorrect_schema.run()

def test_etl_with_incorrect_schemas_can_succeed(spark: SparkSession, incorrect_schema_skip_schema_verification):  # noqa: F811
    """ Schema verification can be skipped and a pipeline with incorrect schema can run. """

    first = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": "1.0"},
        {"id": 2, "name": "Bob", "value": "2.0"},
        {"id": 3, "name": "Cathy", "value": "3.0"},
    ])

    second = spark.createDataFrame([
        {"id": 1, "another_name": "Another Alice", "another_value": "1.0"},
        {"id": 2, "another_name": "Another Bob", "another_value": "2.0"},
        {"id": 3, "another_name": "Another Cathy", "another_value": "3.0"},
    ])

    st = storage.configure()

    st.write(first, "/tmp/simple_join/first", format="delta", mode="overwrite")
    st.write(second, "/tmp/simple_join/second", format="delta", mode="overwrite")

    incorrect_schema_skip_schema_verification.run()


def test_etl_with_merge(spark: SparkSession, merge_delta):   # noqa: F811
    """ The new row is added one row is updated and the missing row is kept. """
    df = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "existing"},
    ])

    st = storage.configure()

    st.write(df, "/tmp/merge/in_df", format="delta", mode="overwrite")
    st.write(df, "/tmp/merge/out", format="delta", mode="overwrite")

    merge_delta.run()

    expected = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "updated"},
        {"id": 4, "value": "added"}
    ])

    result = st.read("/tmp/merge/out", format="delta")

    assertDataFrameEqual(result, expected)
