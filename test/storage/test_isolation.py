import pytest
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
