import pytest

import datetime

from unittest.mock import MagicMock, patch
from jorvik.storage.isolation import IsolatedStorage
from jorvik.storage.basic import BasicStorage
# from jorvik.storage.isolation_providers import get_isolation_provider

@pytest.mark.parametrize(
    "mount_point, isolation_folder, isolation_context, input_path, expected",
    [
        ("", "folder/", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("", "folder", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "folder/", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "//folder/", "//branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/data/", "iso/", "/dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/mnt", "folder", "branch", "/mnt/otherdir/anotherfile.csv", "/mnt/folder/branch/otherdir/anotherfile.csv"),
    ]
)
def test_create_isolation_path_unit(mount_point, isolation_folder, isolation_context, input_path, expected):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        # Mock Spark conf values
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {
            "mount_point": mount_point,
            "isolation_folder": isolation_folder,
        }.get(key)
        mock_spark.sparkContext.getConf().get.side_effect = lambda key, default=None: default
        mock_spark_session.getActiveSession.return_value = mock_spark

        # Mock isolation provider to return given context
        def isolation_provider():
            return isolation_context

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=isolation_provider)
        result = storage._create_isolation_path(input_path)
        assert result == expected


@pytest.mark.parametrize(
    "input_path, isolation_folder, isolation_context, expected",
    [
        ("/mnt/data/file.parquet", "container", "branch", "/mnt/data/file.parquet"),
        ("/mnt/container/branch/data/file.parquet", "container", "branch/", "/mnt/data/file.parquet"),
        ("/mnt/foo/bar/data/file.parquet", "container", "branch/", "/mnt/foo/bar/data/file.parquet"),
    ]
)
def test_remove_isolation_path(input_path, isolation_folder, isolation_context, expected):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {
            "isolation_folder": isolation_folder,
        }.get(key, None)
        mock_spark.sparkContext.getConf().get.side_effect = lambda key, default=None: default
        mock_spark_session.getActiveSession.return_value = mock_spark

        def isolation_provider():
            return isolation_context.strip("/")

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=isolation_provider)
        result = storage._remove_isolation_path(input_path)
        assert result == expected

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
    ]
)
def test_verbose_table_name(input_path, mount_point, expected):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {
            "mount_point": mount_point
        }.get(key)
        mock_spark_session.getActiveSession.return_value = mock_spark

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: "")
        result = storage._verbose_table_name(input_path)
        assert result == expected


@pytest.mark.parametrize(
    "input_path, mount_point, operation, expected_output",
    [
        (
            "/mnt/container/bronze/my_table",
            "",
            "Reading",
            "Reading: container...bronze/my_table .............. path: /mnt/container/bronze/my_table"
        ),
        (
            "/mnt/container/my_table",
            "",
            "Writing",
            "Writing: container...my_table ..................... path: /mnt/container/my_table"
        ),
        (
            "/mnt/data/folder/file",
            "",
            "Saving",
            "Saving: data...folder/file ........................ path: /mnt/data/folder/file"
        ),
        (
            "/custom/bronze/my_table",
            "custom",
            "Listing",
            "Listing: bronze...my_table ........................ path: /custom/bronze/my_table"
        ),
        (
            "/mnt/just_right",
            "",
            "Exploring",
            "Exploring: just_right ............................. path: /mnt/just_right"
        ),
        (
            "/dbfs/container/bronze/table",
            "",
            "Scanning",
            "Scanning: container...bronze/table ................ path: /dbfs/container/bronze/table"
        ),
        (
            "/mnt/",
            "",
            "Inspecting",
            "Inspecting: Unknown ............................... path: /mnt/"
        ),
    ]
)
def test_verbose_print_path(input_path, mount_point, operation, expected_output, capfd):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {"mount_point": mount_point}.get(key)
        mock_spark_session.getActiveSession.return_value = mock_spark

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=lambda: "")
        storage._verbose_print_path(input_path, operation)

        out, _ = capfd.readouterr()
        printed_line = out.strip()
        assert printed_line == expected_output


def test_verbose_print_last_updated(capfd):
    test_path = "/mnt/container/bronze/my_table"
    now = datetime.datetime.now()
    mock_timestamp = now - datetime.timedelta(days=2, hours=5, minutes=13)

    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session, \
            patch("jorvik.storage.isolation.DeltaTable") as mock_delta_table_class, \
            patch("jorvik.storage.isolation.F.col") as mock_col, \
            patch("jorvik.storage.isolation.F.max") as mock_max:

        # Patch Spark session
        mock_spark = MagicMock()
        mock_spark_session.getActiveSession.return_value = mock_spark

        # Patch DeltaTable
        mock_delta_table = MagicMock()
        mock_delta_table_class.forPath.return_value = mock_delta_table

        # Set up the transformation chain mocks
        mock_col.return_value = MagicMock(name="col")
        mock_max.return_value = MagicMock(name="max_col")

        mock_history_df = MagicMock()
        mock_filtered_df = MagicMock()
        mock_limited_df = MagicMock()
        mock_selected_df = MagicMock()
        mock_selected_df.collect.return_value = [[mock_timestamp]]

        mock_delta_table.history.return_value = mock_history_df
        mock_history_df.filter.return_value = mock_filtered_df
        mock_filtered_df.limit.return_value = mock_limited_df
        mock_limited_df.select.return_value = mock_selected_df

        # Run test
        storage = IsolatedStorage(storage=MagicMock(), isolation_provider=lambda: "")
        storage._verbose_print_last_updated(test_path)

        # Capture and verify printed output
        out, _ = capfd.readouterr()
        assert "Table was last updated: 2 days, 5.0 hours, 13.0 minutes ago." in out
