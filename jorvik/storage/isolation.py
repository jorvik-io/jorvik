from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from . import Storage

class IsolatedStorage(Storage):
    def __init__(self, storage: Storage, verbose: bool = False, isolation_provider: Callable = None):
        self.storage = storage
        self.verbose = verbose
        self.isolation_provider = isolation_provider

    def _get_isolation_context(self):

        isolation = self.isolation_provider()

        if not isolation or not isinstance(isolation, str):
            raise ValueError("Isolation path name must be a non-empty string.")

        if isolation.endswith('/') or isolation.endswith('\\'):
            raise ValueError("Isolation path name must not end with '/' or '\\'.")

        if '|' in isolation:
            raise ValueError("Isolation path name must not contain '|'.")

        return isolation

    def _configure_path(self, path: str) -> str:
        """Configure the path based on the isolation context (e.g., branch name)."""
        spark = SparkSession.getActiveSession()

        isolation_container = spark.conf.get("isolation_path")
        if not isolation_container.endswith("/"):
            isolation_container = isolation_container + "/"

        isolation_context = self._get_isolation_context()
        if not isolation_context.endswith("/"):
            isolation_context = isolation_context + "/"

        return path.replace("/mnt", isolation_container + isolation_context)

    def exists(self, path: str) -> bool:
        """Check if the data exists in the isolated path"""
        configured_path = self._configure_path(path)
        return self.storage.exists(configured_path)

    def read(self, path, format=None, options=None):
        configured_path = self._configure_path(path)

        if self.exists(configured_path):
            path = configured_path
        return self.storage.read(path, format, options)

    def readStream(self, path: str, format: str, options: dict = None) -> DataFrame:
        configured_path = self._configure_path(path)

        if self.exists(configured_path):
            path = configured_path

        return self.storage.readStream(configured_path, format, options)

    def read_production_data(self, path, format=None, options=None):
        return self.storage.read(path, format=format, options=options)

    def write(self, df: DataFrame, path: str = None, format: str = None, mode: str = None,
              partition_fields: str | list = "", options: dict = None) -> None:
        configured_path = self._configure_path(path)

        self.storage.write(df, configured_path, format, mode, partition_fields, options)

    def writeStream(self, df: DataFrame, path: str, format: str, checkpoint: str,
                    partition_fields: str | list = "", options: dict = None) -> StreamingQuery:

        configured_path = self._configure_path(path)

        result = self.storage.writeStream(df, configured_path, format, checkpoint, partition_fields, options)
        return result
