from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from . import Storage

class IsolatedStorage(Storage):
    """
    A storage wrapper that isolates data operations based on a context (e.g., branch name).
    """

    def __init__(self, storage: Storage, verbose: bool = False, isolation_provider: Callable = None):
        """
        Initialize IsolatedStorage.

        Args:
            storage (Storage): The underlying storage instance.
            verbose (bool): Enable verbose logging.
            isolation_provider (Callable): Function returning the isolation context (e.g., branch name).
        """
        self.storage = storage
        self.verbose = verbose
        self.isolation_provider = isolation_provider

    def _get_isolation_context(self):
        """
        Get the current isolation context (e.g., branch name).

        Returns:
            str: The isolation context.

        Raises:
            ValueError: If the isolation context is invalid.
        """

        isolation = self.isolation_provider()

        if isolation.endswith('/') or isolation.endswith('\\'):
            raise ValueError("Isolation path name must not end with '/' or '\\'.")

        if '|' in isolation:
            raise ValueError("Isolation path name must not contain '|'.")

        return isolation

    def _configure_path(self, path: str) -> str:
        """
        Configure the path based on the isolation context (e.g., branch name).

        Args:
            path (str): The original storage path.

        Returns:
            str: The isolated storage path.
        """
        spark = SparkSession.getActiveSession()

        isolation_container = spark.conf.get("isolation_path")

        # Ensure the isolation container ends with a slash
        if not isolation_container.endswith("/"):
            isolation_container = isolation_container + "/"

        isolation_context = self._get_isolation_context()

        # Ensure the isolation context ends with a slash
        if not isolation_context.endswith("/"):
            isolation_context = isolation_context + "/"

        return path.replace("/mnt", isolation_container + isolation_context)

    def exists(self, path: str) -> bool:
        """
        Check if the data exists in the isolated path.

        Args:
            path (str): The original storage path.

        Returns:
            bool: True if the data exists, False otherwise.
        """
        configured_path = self._configure_path(path)
        return self.storage.exists(configured_path)

    def read(self, path, format=None, options=None):
        """
        Read data from the isolated path.

        Args:
            path (str): The original storage path.
            format (str, optional): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The DataFrame containing the data.
        """
        configured_path = self._configure_path(path)

        if self.exists(configured_path):
            path = configured_path

        return self.storage.read(path, format, options)

    def readStream(self, path: str, format: str, options: dict = None) -> DataFrame:
        """
        Read streaming data from the isolated path.

        Args:
            path (str): The original storage path.
            format (str): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The streaming DataFrame.
        """
        configured_path = self._configure_path(path)

        if self.exists(configured_path):
            path = configured_path

        return self.storage.readStream(path, format, options)

    def read_production_data(self, path, format=None, options=None):
        """
        Read data from the production (non-isolated) path.

        Args:
            path (str): The storage path.
            format (str, optional): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The DataFrame containing the data.
        """
        return self.storage.read(path, format=format, options=options)

    def write(self, df: DataFrame, path: str = None, format: str = None, mode: str = None,
              partition_fields: str | list = "", options: dict = None) -> None:
        """
        Write data to the isolated path.

        Args:
            df (DataFrame): The DataFrame to write.
            path (str, optional): The storage path.
            format (str, optional): The format to write.
            mode (str, optional): The write mode.
            partition_fields (str or list, optional): Partition fields.
            options (dict, optional): Additional options for writing.
        """
        configured_path = self._configure_path(path)

        self.storage.write(df, configured_path, format, mode, partition_fields, options)

    def writeStream(self, df: DataFrame, path: str, format: str, checkpoint: str,
                    partition_fields: str | list = "", options: dict = None) -> StreamingQuery:
        """
        Write streaming data to the isolated path.

        Args:
            df (DataFrame): The streaming DataFrame to write.
            path (str): The storage path.
            format (str): The format to write.
            checkpoint (str): The checkpoint location.
            partition_fields (str or list, optional): Partition fields.
            options (dict, optional): Additional options for writing.

        Returns:
            StreamingQuery: The streaming query object.
        """

        configured_path = self._configure_path(path)

        result = self.storage.writeStream(df, configured_path, format, checkpoint, partition_fields, options)
        return result
