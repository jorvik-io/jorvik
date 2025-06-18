from tempfile import TemporaryDirectory

from fixtures.spark import *  # noqa: F403 F401

from pyspark.sql import DataFrame, SparkSession
from pyspark.testing import assertDataFrameEqual
import pytest

from jorvik.storage.basic import BasicStorage


# cSpell: words argvalues argnames


@pytest.fixture
def data(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame(
        [
            (1, "Alice", 1.0),
            (2, "Bob", 2.0),
            (3, "Cathy", 3.0),
        ],
        ["id", "name", "value"],
    )


@pytest.mark.parametrize(argnames="data,format",
                         argvalues=[["data", "parquet"],
                                    ["data", "json"],
                                    ["data", "csv"],
                                    ["data", "delta"],
                                    ["data", "orc"]],
                         indirect=["data"])
def test_read_and_write(data: DataFrame, format: str):
    storage = BasicStorage()

    with TemporaryDirectory() as temp_dir:
        storage.write(data, temp_dir, format, "overwrite")
        result_df = storage.read(temp_dir, format)
        if format == "csv":
            result_df = result_df.withColumn("id", result_df["id"].cast("long"))

        assertDataFrameEqual(data, result_df)


@pytest.mark.parametrize(argnames="data,format",
                         argvalues=[["data", "parquet"],
                                    ["data", "json"],
                                    ["data", "delta"],
                                    ["data", "orc"]],
                         indirect=["data"])
def test_read_and_write_streams(data: DataFrame, format: str):
    storage = BasicStorage()

    with TemporaryDirectory() as temp_dir:
        # Write the initial data
        storage.write(data, temp_dir, format, "overwrite")

        # Stream data from the storage
        df = storage.readStream(temp_dir, format)

        with TemporaryDirectory() as new_dir:
            with TemporaryDirectory() as checkpoint:
                # Write the stream to a new location
                storage.writeStream(df, new_dir, format, checkpoint).processAllAvailable()

            # Read result from the new location
            result_df = storage.read(new_dir, format)
            assertDataFrameEqual(data, result_df)


def test_mixed_formats(data: DataFrame):
    storage = BasicStorage()

    with TemporaryDirectory() as temp_dir:
        # Write in CSV format
        storage.write(data, temp_dir, "csv", "overwrite")

        # Read back in Parquet format
        with pytest.raises(Exception):
            storage.read(temp_dir, "parquet")


def test_exists_empty_folder(spark: SparkSession):
    storage = BasicStorage()

    with TemporaryDirectory() as temp_dir:
        assert storage.exists(temp_dir)

    # Path no longer exists after exiting the context manager
    assert not storage.exists(temp_dir)


@pytest.mark.parametrize(argnames="data,format",
                         argvalues=[["data", "parquet"],
                                    ["data", "json"],
                                    ["data", "csv"],
                                    ["data", "delta"],
                                    ["data", "orc"]],
                         indirect=["data"])
def test_exists(data: DataFrame, format: str):
    storage = BasicStorage()

    with TemporaryDirectory() as temp_dir:
        storage.write(data, temp_dir, "delta", "overwrite")
        assert storage.exists(temp_dir)

    # Path no longer exists after exiting the context manager
    assert not storage.exists(temp_dir)
