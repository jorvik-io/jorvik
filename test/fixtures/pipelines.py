import pytest
from jorvik.pipelines import etl, FileInput, FileOutput, MergeDeltaOutput

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType
from pyspark.sql import SparkSession, DataFrame, functions as F


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
    def transform(first: DataFrame, second: DataFrame):
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
    def transform(first: DataFrame, second: DataFrame):
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
    def transform(first: DataFrame, second: DataFrame):
        return first.join(second, on=["id"], how="inner")

    return transform


@pytest.fixture
def simple_join_without_schemas():
    first = FileInput(path="/tmp/simple_join/first", format="delta")

    second = FileInput(path="/tmp/simple_join/second", format="delta")

    out = FileOutput(path="/tmp/simple_join/out", format="delta", mode="overwrite")

    @etl(inputs=[first, second], outputs=[out])
    def transform(first: DataFrame, second: DataFrame):
        return first.join(second, on=["id"], how="inner")

    return transform


@pytest.fixture
def merge_delta():
    in_df = FileInput(path="/tmp/merge/in_df", format="delta")
    out = MergeDeltaOutput(path="/tmp/merge/out", merge_condition="full.id = incremental.id")

    @etl(inputs=[in_df], outputs=[out])
    def transform(df: DataFrame):
        """ Adds a new row (id = 4), update a row ( id = 3) and drop a row ( id = 1 ). """
        spark = SparkSession.getActiveSession()
        df2 = spark.createDataFrame([{"id": 4, "value": "added"}])
        df = df.unionByName(df2)
        df = df.withColumn("value", F.when(F.col("id") == 3, "updated").otherwise(F.col("value")))
        df = df.filter("id != 1")
        return df

    return transform
