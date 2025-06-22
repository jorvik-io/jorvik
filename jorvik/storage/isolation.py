from typing import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from jorvik.storage import Storage
from jorvik.storage.basic import BasicStorage

class IsolatedStorage:
    def __init__(self, storage: Storage, verbose: bool = False, isolation_provider: Callable = None):
        self.storage = storage
        self.verbose = verbose
        self.isolation_provider = isolation_provider

    def _values_to_list(self, value: str) -> list:
        """ Normalize any user input into a clean list of values. """
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        else:
            raise ValueError("Expected input to be a single or comma separated string")

    def _configure_path(self, path: str) -> str:
        """Configure the path based on the isolation context (e.g., branch name)."""
        spark = SparkSession.getActiveSession()

        isolation = self.isolation_provider().replace("/", "_") # branch name

        isolation_container = spark.conf.get("isolation_path")
        prod_paths = self._values_to_list(spark.conf.get("prod_path"))

        active_prod = [x for x in prod_paths if x in path]

        isolation_path = path.replace(*active_prod, isolation_container + "/" + isolation)
        return isolation_path

    def read(self, path, format=None, options=None):
        configured_path = self._configure_path(path)
        if BasicStorage.exists(configured_path):
            path = configured_path
        return self.storage.read(path, format=format, options=options)

    def read_production_data(self, path, format=None, options=None):
        return self.storage.read(path, format=format, options=options)
    
    def write(self, df: DataFrame, path: str=None, format: str=None, mode: str=None,
            partition_fields: str | list = "", options: dict = None) -> None:
        
        configured_path = self._configure_path(path)

        writer = df.write.format(format).mode(mode)

        if options:
            writer = writer.options(**options)

        if partition_fields:
            writer = writer.partitionBy(partition_fields)

        writer.save(configured_path)
