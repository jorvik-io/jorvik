import pytest
from unittest.mock import MagicMock, patch
from jorvik.storage.isolation import IsolatedStorage
from jorvik.storage.basic import BasicStorage

@pytest.mark.parametrize(
    "mount_point, isolation_folder, isolation_provider, input_path, expected",
    [
        ("", "folder/", "branch/", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("", "folder", "branch", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "folder/", "branch/", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("/mnt/", "//folder/", "//branch/", "/mnt/data/file.parquet", "/mnt/folder/branch/data/file.parquet"),
        ("data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/data", "iso", "dev", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/data/", "iso/", "/dev/", "/data/file.parquet", "/data/iso/dev/file.parquet"),
        ("/mnt", "folder", "branch", "/mnt/otherdir/anotherfile.csv", "/mnt/folder/branch/otherdir/anotherfile.csv"),
    ]
)
def test_create_isolation_path(mount_point, isolation_folder, isolation_provider, input_path, expected):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {
            "mount_point": mount_point,
            "isolation_folder": isolation_folder
        }.get(key, None)
        mock_spark_session.getActiveSession.return_value = mock_spark

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=isolation_provider)
        result = storage._create_isolation_path(input_path)
        assert result == expected


@pytest.mark.parametrize(
    "input_path, isolation_folder, isolation_provider, expected",
    [
        ("/mnt/data/file.parquet", "container", "branch", "/mnt/data/file.parquet"),
        ("/mnt/container/branch/data/file.parquet", "container", "branch/", "/mnt/data/file.parquet"),
        ("/mnt/foo/bar/data/file.parquet", "container", "branch/", "/mnt/foo/bar/data/file.parquet"),
    ]
)
def test_remove_isolation_path(input_path, isolation_folder, isolation_provider, expected):
    with patch("jorvik.storage.isolation.SparkSession") as mock_spark_session:
        mock_spark = MagicMock()
        mock_spark.conf.get.side_effect = lambda key: {
            "isolation_folder": isolation_folder,
            "isolation_provider": isolation_provider
        }.get(key, None)
        mock_spark_session.getActiveSession.return_value = mock_spark

        storage = IsolatedStorage(storage=BasicStorage(), isolation_provider=isolation_provider)
        result = storage._remove_isolation_path(input_path)
        assert result == expected