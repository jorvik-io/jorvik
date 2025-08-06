import pytest
import datetime

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery

from unittest.mock import MagicMock, patch

from jorvik.storage.isolation import IsolatedStorage
from jorvik.storage.basic import BasicStorage


@pytest.fixture
def mock_storage():
    return MagicMock()


@pytest.fixture
def isolated_storage(mock_storage):
    return IsolatedStorage(
        storage=mock_storage,
        verbose=False,
        isolation_provider=lambda: "test-branch"
    )


@pytest.fixture
def mock_spark_conf():
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()

        # Ensure conf.get returns strings
        mock_spark.conf.get.side_effect = lambda key, default=None: {
            "io.jorvik.storage.mount_point": "/mnt",
            "io.jorvik.storage.isolation_folder": "isolated"
        }.get(key, default)

        # Also mock sparkContext.getConf().get
        mock_spark.sparkContext.getConf().get.side_effect = lambda key, default=None: {
            "io.jorvik.storage.mount_point": "/mnt",
            "io.jorvik.storage.isolation_folder": "isolated"
        }.get(key, default)

        mock_spark_session.getActiveSession.return_value = mock_spark
        yield mock_spark


@pytest.mark.parametrize(
    "mount_point, isolation_folder, isolation_context, input_path, expected",
    [
        ("", "folder/", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("", "folder", "/branch/", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "folder/", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "/folder/", "/branch/", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
    ]
)
def test_create_isolation_path(mock_spark_conf, mount_point, isolation_folder, isolation_context, input_path, expected):
    mock_spark_conf.conf.get.side_effect = lambda key, default=None: {
        "io.jorvik.storage.mount_point": mount_point,
        "io.jorvik.storage.isolation_folder": isolation_folder,
    }.get(key, default)

    storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: isolation_context)
    assert storage._create_isolation_path(input_path) == expected


@pytest.mark.parametrize(
    "input_path, isolation_folder, isolation_context, expected",
    [
        ("/mnt/data/file.parquet", "container", "branch", "/mnt/data/file.parquet"),
        ("/mnt/container/branch/data/file.parquet", "container", "branch/", "/mnt/data/file.parquet"),
        ("/mnt/foo/bar/data/file.parquet", "container", "branch/", "/mnt/foo/bar/data/file.parquet"),
    ]
)
def test_remove_isolation_path(mock_spark_conf, input_path, isolation_folder, isolation_context, expected):
    mock_spark_conf.conf.get.side_effect = lambda key: {"io.jorvik.storage.isolation_folder": isolation_folder}.get(key)

    storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: isolation_context.strip("/"))
    assert storage._remove_isolation_path(input_path) == expected


@pytest.mark.parametrize(
    "input_path, mount_point, expected",
    [
        ("/mnt/folder/bronze/my_table", "", "folder...bronze/my_table"),
        ("/dbfs///folder/bronze/foo/bar/table", "", "folder...bar/table"),
        ("/mnt/data/folder/file/////", "", "data...folder/file"),
        ("/mnt/bronze/my_table", "", "bronze...my_table"),
        ("/mnt/justone", "", "justone"),
        ("/mnt/", "", "Unknown"),
        ("", "", "Unknown"),
        ("/", "", "Unknown"),
    ]
)
def test_verbose_table_name(mock_spark_conf, input_path, mount_point, expected):
    mock_spark_conf.conf.get.side_effect = lambda key, default=None: {
        "io.jorvik.storage.mount_point": mount_point}.get(key, default)
    storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: "")
    assert storage._verbose_table_name(input_path) == expected


@pytest.mark.parametrize(
    "input_path, mount_point, operation, expected_output",
    [
        ("/mnt/container/bronze/my_table", "", "Reading",
         "Reading: container...bronze/my_table .............. path: /mnt/container/bronze/my_table"),
        ("/mnt/container/my_table", "", "Writing",
         "Writing: container...my_table ..................... path: /mnt/container/my_table"),
        ("/mnt/data/folder/file", "", "Saving",
         "Saving: data...folder/file ........................ path: /mnt/data/folder/file"),
        ("/custom/bronze/my_table", "custom", "Listing",
         "Listing: bronze...my_table ........................ path: /custom/bronze/my_table"),
        ("/mnt/just_right", "", "Exploring",
         "Exploring: just_right ............................. path: /mnt/just_right"),
        ("/dbfs/container/bronze/table", "", "Scanning",
         "Scanning: container...bronze/table ................ path: /dbfs/container/bronze/table"),
        ("/mnt/", "", "Inspecting", "Inspecting: Unknown ............................... path: /mnt/"),
    ]
)
def test_verbose_print_path(mock_spark_conf, input_path, mount_point, operation, expected_output, capfd):
    mock_spark_conf.conf.get.side_effect = lambda key, default=None: {
        "io.jorvik.storage.mount_point": mount_point}.get(key, default)
    storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: "")
    storage._verbose_print_path(input_path, operation)

    out, _ = capfd.readouterr()
    assert out.strip() == expected_output


def test_verbose_print_last_updated(mock_spark_conf, capfd):
    test_path = "/mnt/container/bronze/my_table"
    now = datetime.datetime.now()
    mock_timestamp = now - datetime.timedelta(days=2, hours=5, minutes=13)

    with patch("jorvik.storage.isolation.DeltaTable") as mock_delta_table_class, \
            patch("jorvik.storage.isolation.F.col") as mock_col, \
            patch("jorvik.storage.isolation.F.max") as mock_max:

        mock_delta_table = MagicMock()
        mock_delta_table_class.forPath.return_value = mock_delta_table

        mock_col.return_value = MagicMock()
        mock_max.return_value = MagicMock()

        mock_selected_df = MagicMock()
        mock_selected_df.collect.return_value = [[mock_timestamp]]

        mock_delta_table.history.return_value.filter.return_value.limit.return_value.select.return_value = mock_selected_df

        storage = IsolatedStorage(storage=MagicMock(), verbose=True, isolation_provider=lambda: "")
        storage._verbose_print_last_updated(test_path)

        out, _ = capfd.readouterr()
        assert "Table was last updated: 2 days, 5 hours, 13 minutes ago." in out

        mock_selected_df.collect.return_value = [[None]]
        storage._verbose_print_last_updated(test_path)

        out, _ = capfd.readouterr()
        assert "No WRITE, MERGE, or STREAMING operations found in Delta table history." in out


@patch.object(IsolatedStorage, "_verbose_print_path")
@patch.object(IsolatedStorage, "_verbose_print_last_updated")
def test_verbose_output_triggers_last_updated_for_reading_delta(mock_last_updated, mock_print_path):
    storage = IsolatedStorage(storage=MagicMock(), isolation_provider=lambda: "test", verbose=True)
    storage._verbose_output("/mnt/data/my_table", operation="Reading", format="delta")

    mock_print_path.assert_called_once_with("/mnt/data/my_table", "Reading")
    mock_last_updated.assert_called_once_with("/mnt/data/my_table")


@patch.object(IsolatedStorage, "_verbose_print_path")
@patch.object(IsolatedStorage, "_verbose_print_last_updated")
@pytest.mark.parametrize("operation,format", [
    ("Reading", "parquet"),
    ("Merging", "csv"),
    ("Writing", "delta"),
    ("Writing", "parquet")
])
def test_verbose_output_skips_last_updated_when_not_reading_merging_delta(
        mock_last_updated, mock_print_path, operation, format):
    storage = IsolatedStorage(storage=MagicMock(), isolation_provider=lambda: "test", verbose=True)
    storage._verbose_output("/mnt/data/my_table", operation=operation, format=format)

    mock_print_path.assert_called_once_with("/mnt/data/my_table", operation)
    mock_last_updated.assert_not_called()


def test_exists_calls_storage_with_direct_path(isolated_storage, mock_storage):
    mock_storage.exists.return_value = True

    result = isolated_storage.exists("/mnt/data/table")

    assert result is True
    mock_storage.exists.assert_called_once_with("/mnt/data/table")


def test_read_prefers_isolated_path_if_exists(isolated_storage, mock_storage):
    mock_storage.exists.return_value = True
    mock_storage.read.return_value = MagicMock(spec=DataFrame)

    with patch.object(isolated_storage, "_create_isolation_path", return_value="/mnt/iso-path"):
        isolated_storage.read("/mnt/original", format="delta")

    mock_storage.read.assert_called_once_with("/mnt/iso-path", "delta", None)


def test_read_stream_uses_isolated_path_if_exists(isolated_storage, mock_storage):
    mock_storage.exists.return_value = True
    mock_storage.readStream.return_value = MagicMock(spec=DataFrame)

    with patch.object(isolated_storage, "_create_isolation_path", return_value="/mnt/stream-path"):
        isolated_storage.readStream("/mnt/original", format="delta")

    mock_storage.readStream.assert_called_once_with("/mnt/stream-path", "delta", None)


def test_read_production_data_reads_from_clean_path(isolated_storage, mock_storage):
    mock_storage.read.return_value = MagicMock(spec=DataFrame)

    with patch.object(isolated_storage, "_remove_isolation_path", return_value="/mnt/clean-path"):
        isolated_storage.read_production_data("/mnt/something", format="delta")

    mock_storage.read.assert_called_once_with("/mnt/clean-path", format="delta", options=None)


def test_write_calls_storage_with_isolated_path(isolated_storage, mock_storage):
    mock_df = MagicMock(spec=DataFrame)

    with patch.object(isolated_storage, "_create_isolation_path", return_value="/mnt/write-path"):
        isolated_storage.write(mock_df, "/mnt/original", format="delta", mode="overwrite")

    mock_storage.write.assert_called_once_with(mock_df, "/mnt/write-path", "delta", "overwrite", "", None)


def test_write_stream_calls_storage_with_isolated_path(isolated_storage, mock_storage):
    mock_df = MagicMock(spec=DataFrame)
    mock_query = MagicMock(spec=StreamingQuery)
    mock_storage.writeStream.return_value = mock_query

    with patch.object(isolated_storage, "_create_isolation_path", return_value="/mnt/stream-write"):
        result = isolated_storage.writeStream(mock_df, "/mnt/original", format="delta", checkpoint="/mnt/checkpoint")

    mock_storage.writeStream.assert_called_once_with(mock_df, "/mnt/stream-write", "delta", "/mnt/checkpoint", "", None)
    assert result is mock_query


def test_merge_calls_storage_merge(isolated_storage, mock_storage):
    mock_df = MagicMock(spec=DataFrame)
    mock_storage.exists.return_value = True

    with patch.object(isolated_storage, "_create_isolation_path", return_value="/mnt/merge-path"):
        isolated_storage.merge(
            mock_df,
            path="/mnt/original",
            merge_condition="source.id = target.id",
            partition_fields=["date"],
            merge_schemas=True,
            update_condition="source.updated_at > target.updated_at",
            insert_condition=True
        )

    mock_storage.merge.assert_called_once_with(
        mock_df,
        "/mnt/merge-path",
        "source.id = target.id",
        ["date"],
        True,
        "source.updated_at > target.updated_at",
        True
    )

def test_e2e_isolated_storage_write_read_with_mocks(
    isolated_storage: IsolatedStorage,
    mock_storage: MagicMock,
    mock_spark_conf: MagicMock
):
    # Configure the mock to return a DataFrame when read is called
    mock_read_df = MagicMock(spec=DataFrame)
    mock_storage.read.return_value = mock_read_df

    # Set up isolation path and mock the existence check
    full_path = "/mnt/data/my_table"
    isolated_path = "/mnt/isolated/test-branch/data/my_table"

    # Mock _create_isolation_path to return the isolated path
    with patch.object(isolated_storage, "_create_isolation_path", return_value=isolated_path):

        # Test the write operation
        mock_df_to_write = MagicMock(spec=DataFrame)
        isolated_storage.write(mock_df_to_write, full_path, format="delta", mode="overwrite")

        # Assert that the underlying storage's write method was called with the isolated path
        mock_storage.write.assert_called_once_with(
            mock_df_to_write,
            isolated_path,
            "delta",
            "overwrite",
            "",
            None
        )

        # Reset mock for the next assertion
        mock_storage.write.reset_mock()

        # Test the read operation
        # Assume isolated path exists and production path does not
        mock_storage.exists.side_effect = lambda path: path == isolated_path

        read_df = isolated_storage.read(full_path, format="delta")

        # Assert that the underlying storage's read method was called with the isolated path
        mock_storage.read.assert_called_once_with(isolated_path, "delta", None)

        # Assert that the method returns the mocked DataFrame
        assert read_df is mock_read_df

        # Reset mocks
        mock_storage.exists.reset_mock()
        mock_storage.read.reset_mock()

        # Test the case where the isolated path doesn't exist, so it falls back to the original
        mock_storage.exists.side_effect = lambda path: False

        read_df_fallback = isolated_storage.read(full_path, format="delta")

        # Assert that the storage's read method was called with the original path
        mock_storage.read.assert_called_once_with(full_path, "delta", None)
        assert read_df_fallback is mock_read_df
