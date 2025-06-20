import pytest
from jorvik.pipelines import etl, FileInput, FileOutput

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


@pytest.fixture
def simple_join():
    first = FileInput(
        path="/tmp/simple_join/first",
        format="delta",
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
        ]))

    second = FileInput(
        path="/tmp/simple_join/second",
        format="delta",
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("another_name", StringType(), True),
            StructField("another_value", StringType(), True),
        ]))

    out = FileOutput(
        path="/tmp/simple_join/out", format="delta", mode="overwrite",
        schema=StructType([
            StructField("id", LongType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
            StructField("another_name", StringType(), True),
            StructField("another_value", StringType(), True),
        ])
    )

    @etl(inputs=[first, second], outputs=[out])
    def transform(first, second):
        return first.join(second, on=["id"], how="inner")

    return transform


@pytest.fixture
def incorrect_schema():
    first = FileInput(
        path="/tmp/simple_join/first", format="delta",
        schema=StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
        ]))

    second = FileInput(
        path="/tmp/simple_join/second", format="delta",
        schema=StructType([
            StructField("id", IntegerType(), True),
            StructField("another_name", StringType(), True),
            StructField("another_value", StringType(), True),
        ]))

    out = FileOutput(
        path="/tmp/simple_join/out", format="delta", mode="overwrite",
        schema=StructType([
            StructField("column1", StringType(), True),
            StructField("column2", StringType(), True)
        ]))

    @etl(inputs=[first, second], outputs=[out])
    def transform(first, second):
        return first.join(second, on=["id"], how="inner")

    return transform


@pytest.fixture
def incorrect_schema_skip_schema_verification():
    first = FileInput(
        path="/tmp/simple_join/first", format="delta",
        schema=StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("value", StringType(), True),
        ]))

    second = FileInput(
        path="/tmp/simple_join/second", format="delta",
        schema=StructType([
            StructField("id", IntegerType(), True),
            StructField("another_name", StringType(), True),
            StructField("another_value", StringType(), True),
        ]))

    out = FileOutput(
        path="/tmp/simple_join/out", format="delta", mode="overwrite",
        schema=StructType([
            StructField("column1", StringType(), True),
            StructField("column2", StringType(), True)
        ]))

    @etl(inputs=[first, second], outputs=[out], validate_schemas=False)
    def transform(first, second):
        return first.join(second, on=["id"], how="inner")

    return transform


@pytest.fixture
def simple_join_without_schemas():
    first = FileInput(path="/tmp/simple_join/first", format="delta")

    second = FileInput(path="/tmp/simple_join/second", format="delta")

    out = FileOutput(path="/tmp/simple_join/out", format="delta", mode="overwrite")

    @etl(inputs=[first, second], outputs=[out])
    def transform(first, second):
        return first.join(second, on=["id"], how="inner")

    return transform
