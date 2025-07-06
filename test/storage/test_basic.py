from tempfile import TemporaryDirectory


from pyspark.sql import DataFrame
from pyspark.testing import assertDataFrameEqual
import pytest

from jorvik.storage.basic import BasicStorage
from jorvik.pipelines.testing import spark, SparkSession  # noqa: F401


# cSpell: words argvalues argnames


@pytest.fixture
def data(spark: SparkSession) -> DataFrame:  # noqa: F811
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


def test_exists_empty_folder(spark: SparkSession):  # noqa: F811
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
        storage.write(data, temp_dir, format, "overwrite")
        assert storage.exists(temp_dir)

    # Path no longer exists after exiting the context manager
    assert not storage.exists(temp_dir)


def test_merge(spark: SparkSession):   # noqa: F811
    """  The new rows are added when the insert condition is met
         the updated rows are added when the update_condition is met
         and the missing rows are kept. """

    df = spark.createDataFrame([
        {"id": 1, "value": "existing"},  # Missing from incremental
        {"id": 2, "value": "existing"},  # Update condition not met
        {"id": 3, "value": "existing"},  # Will be updated
    ])

    storage = BasicStorage()

    storage.write(df, "/tmp/merge/data", format="delta", mode="overwrite")

    incremental = spark.createDataFrame([
        {"id": 2, "value": "updated"},  # Update condition not met
        {"id": 3, "value": "updated"},  # Will be updated
        {"id": 4, "value": "added"},  # Insert condition not met
        {"id": 5, "value": "added"},  # Will be inserted
    ])

    storage.merge(incremental, "/tmp/merge/data", merge_condition="full.id == incremental.id",
                  insert_condition="incremental.id != 4", update_condition="incremental.id != 2")

    expected = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "updated"},
        {"id": 5, "value": "added"}
    ])

    result = storage.read("/tmp/merge/data", format="delta")

    assertDataFrameEqual(result, expected)


def test_merge_with_schema_changes(spark: SparkSession):   # noqa: F811
    """  If there are schema changes an error is thrown unless the merge_schema is set to True,
         which will force schemas to be merged. """
    df = spark.createDataFrame([
        {"id": 1, "value": "existing", "old": 1},
        {"id": 2, "value": "existing", "old": 1},
        {"id": 3, "value": "existing", "old": 1},
    ])

    storage = BasicStorage()

    storage.write(df, "/tmp/merge/data", format="delta", mode="overwrite")

    incremental = spark.createDataFrame([
        {"id": 3, "value": "updated", "new": 2},
        {"id": 4, "value": "added", "new": 2},

    ])

    with pytest.raises(ValueError):
        storage.merge(incremental, "/tmp/merge/data", merge_condition="full.id == incremental.id")

    storage.merge(incremental, "/tmp/merge/data", merge_condition="full.id == incremental.id", merge_schemas=True)

    expected = spark.createDataFrame([
        {"id": 1, "value": "existing", "old": 1, "new": None},
        {"id": 2, "value": "existing", "old": 1, "new": None},
        {"id": 3, "value": "updated", "old": None, "new": 2},
        {"id": 4, "value": "added", "old": None, "new": 2}
    ])

    result = storage.read("/tmp/merge/data", format="delta")

    # Ignore column order
    result = result.select("id", "value", "old", "new")
    expected = expected.select("id", "value", "old", "new")

    assertDataFrameEqual(result, expected)


def test_merge_ignore_updates(spark: SparkSession):   # noqa: F811
    """ Only new rows are inserted, updates are ignored. """
    df = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "existing"},
    ])

    storage = BasicStorage()

    storage.write(df, "/tmp/merge/data", format="delta", mode="overwrite")

    incremental = spark.createDataFrame([
        {"id": 3, "value": "updated"},
        {"id": 4, "value": "added"},
    ])

    storage.merge(incremental, "/tmp/merge/data", merge_condition="full.id == incremental.id",
                  update_condition=False)

    expected = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "existing"},
        {"id": 4, "value": "added"}
    ])

    result = storage.read("/tmp/merge/data", format="delta")

    assertDataFrameEqual(result, expected)


def test_merge_ignore_inserts(spark: SparkSession):   # noqa: F811
    """ Only existing rows are updated, inserts are ignored. """
    df = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "existing"},
    ])

    storage = BasicStorage()

    storage.write(df, "/tmp/merge/data", format="delta", mode="overwrite")

    incremental = spark.createDataFrame([
        {"id": 3, "value": "updated"},
        {"id": 4, "value": "added"},
    ])

    storage.merge(incremental, "/tmp/merge/data", merge_condition="full.id == incremental.id",
                  insert_condition=False)

    expected = spark.createDataFrame([
        {"id": 1, "value": "existing"},
        {"id": 2, "value": "existing"},
        {"id": 3, "value": "updated"}
    ])

    result = storage.read("/tmp/merge/data", format="delta")

    assertDataFrameEqual(result, expected)


def test_inserts_and_updates_are_ignored():
    """ Raises an error if both insert and update condition are set to False. """
    storage = BasicStorage()

    with pytest.raises(ValueError):
        storage.merge(None, "", "", insert_condition=False, update_condition=False)
