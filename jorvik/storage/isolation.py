from typing import Callable
import datetime
import re

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from delta.tables import DeltaTable

from jorvik.storage.basic import BasicStorage

class IsolatedStorage():
    """
    A storage wrapper that isolates data operations based on a context (e.g., branch name).
    """

    def __init__(self, storage: BasicStorage, verbose: bool = False, isolation_provider: Callable = None):
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

    def _create_isolation_path(self, path: str) -> str:
        """
        Create the isolation path based on the provided path and isolation context.

        Args:
            path (str): The original storage path.

        Returns:
            str: The full isolation path.
        """
        spark = SparkSession.getActiveSession()

        mount_point = spark.conf.get("mount_point")
        if not mount_point:
            mount_point = "/mnt/"

        isolation_container = spark.conf.get("isolation_path")
        isolation_context = self.isolation_provider

        # Ensure the isolation container ends with a slash
        if not isolation_container.endswith("/"):
            isolation_container = isolation_container + "/"

        # Ensure the isolation context ends with a slash
        if not isolation_context.endswith("/"):
            isolation_context = isolation_context + "/"

        # Replace the mount point with the isolation container and context
        full_isolation_path = path.replace(mount_point, mount_point + isolation_container + self.isolation_provider)
        full_isolation_path = re.sub('/+', '/', full_isolation_path)  # Ensure single slashes

        return full_isolation_path

    def _remove_isolation_path(self, path: str) -> str:
        """
        Remove the isolation path from the provided path.

        Args:
            path (str): The original storage path.

        Returns:
            str: The path without the isolation context.
        """
        spark = SparkSession.getActiveSession()

        isolation_container = spark.conf.get("isolation_path")

        return path.replace(isolation_container + self.isolation_provider, "")

    def _verbose_print_last_updated(self, path: str) -> None:
        """
        Prints a human-readable message indicating how long ago a Delta Lake table at the specified path was last updated.

        This method examines the Delta table's operation history to determine the most recent update time:
        - For batch tables, it considers the latest 'WRITE' or 'MERGE' operation.
        - For streaming tables, it includes 'STREAMING' operations.

        The elapsed time since the last update is printed in days, hours, and minutes for batch tables,
        or in seconds for streaming tables.

        Args:
            path (str): The file system path to the Delta Lake table.

        Example output:
            Table was last updated: 2 days, 5 hours, 13 minutes ago.
        """
        spark = SparkSession.getActiveSession()

        # Initialize DeltaTable object for the given path
        delta_table = DeltaTable.forPath(spark, path)

        update_ts = (
            delta_table.history()
            .filter(F.col("operation").isin(["WRITE", "MERGE", "STREAMING"]))
            .limit(1)
            .select(F.max(F.col("timestamp")).alias("latest_update"))
            .collect()[0][0]
        )

        if update_ts:
            time_difference = datetime.datetime.now() - update_ts
            total_seconds = time_difference.total_seconds()
            days = time_difference.days
            hours = (total_seconds // 3600) % 24
            minutes = (total_seconds // 60) % 60

            print(f"Table was last updated: {days} days, {hours} hours, {minutes} minutes ago.\n")
        else:
            print("No WRITE, MERGE, or STREAMING operations found in Delta table history.\n")

    def _verbose_table_name(self, path: str) -> str:
        """
        Extracts table name from a given path string.

        The method processes the input path to determine a human-readable table name:
        - If the path ends with a slash ("/"), it is removed.
        - The path is split into parts using "/" as the delimiter.
        - If the path ends with "/data", the method returns the name of the parent directory (the second-to-last part).
        - Otherwise, it returns the last part of the path.
        - If the resulting name is empty, "Unknown" is returned.

        Args:
            path (str): The file or directory path from which to extract the table name.

        Returns:
            str: The extracted table name, or "Unknown" if it cannot be determined.

        Example:
            >>> _verbose_table_name("/foo/bar/data")
            'bar'
            >>> _verbose_table_name("/foo/bar/")
            'bar'
            >>> _verbose_table_name("/foo/bar")
            'bar'
            >>> _verbose_table_name("/")
            'Unknown'
        """
        # Ensure the path does not end with a slash
        if path.endswith("/"):
            path = path[:-1]

        parts = path.split("/")
        # If the path ends with "/data", return the second-to-last part
        if path.endswith("/data"):
            result = parts[-2] if len(parts) > 1 else ""
        else:
            result = parts[-1] if parts else ""

        if not result:
            return "Unknown"
        else:
            return result

    def _verbose_print_path(self, path: str, operation: str) -> None:
        """
        Prints the operation and path in a verbose, formatted manner for debugging.

        Args:
            path (str): The file or resource path to be printed.
            operation (str): The operation being performed (e.g., 'read', 'write').

        Returns:
            None

        # Inline comments:
        # - table_name: Extracts a human-readable table name from the path.
        # - dots: Fills the space between table_name and path for alignment, up to 40 characters.
        # - print: Outputs the formatted string showing the operation, table name, and path.
        """
        table_name = self._verbose_table_name(path)
        dots = '.' * (40 - len(table_name)) if len(table_name) < 40 else ' '
        print(f"{operation}: {table_name} {dots} path: {path}")

    def _verbose_output(self, path: str, operation: str):

        self._verbose_print_path(path, operation)

        if operation == "Reading":
            self._verbose_print_last_updated(path)

    def exists(self, path: str) -> bool:
        """
        Check if the data exists in the isolated path.

        Args:
            path (str): The original storage path.

        Returns:
            bool: True if the data exists, False otherwise.
        """
        isolation_path = self._create_isolation_path(path)
        return self.storage.exists(isolation_path)

    def read(self, path, format=None, options=None) -> DataFrame:
        """
        Read data from the given path. If an isolated path exists, read from there.

        Args:
            path (str): The original storage path.
            format (str, optional): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The DataFrame containing the data.
        """
        isolation_path = self._create_isolation_path(path)

        if self.exists(isolation_path):
            path = isolation_path

        if self.verbose:
            self._verbose_output(path, "Reading")

        return self.storage.read(path, format, options)

    def readStream(self, path: str, format: str, options: dict = None) -> DataFrame:
        """
        Read streaming data from the isolated path if it exists.
        Otherwise read from the original path.

        Args:
            path (str): The original storage path.
            format (str): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The streaming DataFrame.
        """
        isolation_path = self._create_isolation_path(path)

        if self.exists(isolation_path):
            path = re.sub('/+', '/', isolation_path)

        if self.verbose:
            self._verbose_output(path, "Reading")

        return self.storage.readStream(path, format, options)

    def read_production_data(self, path, format=None, options=None) -> DataFrame:
        """
        Read data from the production (non-isolated) path.
        This method reads data from the original path without considering isolation.
        If isolation is provided in the path, it will be removed.

        Args:
            path (str): The storage path.
            format (str, optional): The format of the data.
            options (dict, optional): Additional options for reading.

        Returns:
            DataFrame: The DataFrame containing the data.
        """
        configured_path = self._remove_isolation_path(path)

        if self.verbose:
            self._verbose_output(configured_path, "Reading")

        return self.storage.read(configured_path, format=format, options=options)

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
        isolation_path = self._create_isolation_path(path)

        if self.verbose:
            self._verbose_output(path, "Writing")

        self.storage.write(df, isolation_path, format, mode, partition_fields, options)

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

        isolation_path = self._create_isolation_path(path)

        if self.verbose:
            self._verbose_output(path, "Writing")

        return self.storage.writeStream(df, isolation_path, format, checkpoint, partition_fields, options)
