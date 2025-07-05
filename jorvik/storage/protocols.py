from typing import Protocol
from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery


class StorageOutputObserver(Protocol):
    def update(self, df: DataFrame, output_path: str) -> None:
        """ Action when Storage subject notifies an event of writing a DataFrame to a path.
            Args:
                df (DataFrame): The DataFrame to write.
                output_path (str): The path to write the data to.
        """
        ...


class Storage(Protocol):
    def read(self, path: str, format: str, options: dict = None) -> DataFrame:
        """ Read data from the storage.

            Args:
                path (str): The path to the data.
                format (str): The format of the data. Available formats are:
                    - delta
                    - parquet
                    - json
                    - csv
                    - orc
                options (dict): Additional options for reading.
            Returns:
                DataFrame: The DataFrame containing the data.
        """
        ...

    def readStream(self, path: str, format: str, options: dict = None) -> DataFrame:
        """ Stream data from the storage.

            Args:
                path (str): The path to the data.
                format (str): The format of the data. Available formats are:
                    - delta
                    - parquet
                    - json
                    - orc
                options (dict): Additional options for reading.
            Returns:
                DataFrame: The DataFrame containing the data.
        """
        ...

    def write(self, df: DataFrame, path: str, format: str, mode: str,
              partition_fields: str | list = "", options: dict = None) -> None:
        """ Write data to the storage.

            Args:
                df (DataFrame): The DataFrame to write.
                path (str): The path to write the data to. Available formats are:
                    - delta
                    - parquet
                    - json
                    - csv
                    - orc
                format (str): The format of the data.
                mode (str): The write mode.
                partition_fields (str | list): The fields to partition by.
                options (dict): Additional options for writing. Default is None.
        """
        ...

    def writeStream(self, df: DataFrame, path: str, format: str, checkpoint: str,
                    partition_fields: str | list = "", options: dict = None) -> StreamingQuery:
        """ Stream data to the storage.

            Args:
                df (DataFrame): The DataFrame to write.
                path (str): The path to write the data to. Available formats are:
                    - delta
                    - parquet
                    - json
                    - orc
                format (str): The format of the data.
                checkpoint (str): The checkpoint location.
                partition_fields (str | list): The fields to partition by.
                options (dict): Additional options for writing. Default is None.
        """
        ...

    def merge(self, df: DataFrame, path: str, merge_condition: str, partition_fields: str | list = "",
              merge_schemas: bool = False, update_condition: str | bool = None, insert_condition: str | bool = None) -> None:
        """ Merge incremental data to full data. Only applicable for Delta tables.
            (Full) delta table and given dataframe are aliased 'full' and 'incremental' respectively,
            you will need to use these aliases in the merge condition.

            Args:
                df (DataFrame): Dataframe containing incremental data.
                path (str): path to Delta table.
                merge_condition (str): condition to merge incremental to full data
                    sample: 'full.id = incremental.id'
                partition_fields (str | list): The fields to partition by.
                merge_schemas (bool): if True it sets existing fields that are missing on the incremental data
                    and new fields that are missing from the full data to None.
                    Else it throws a ValueError if the schema has changed.
                    Defaults to False.
                update_condition (str | bool): optional condition of the update. Defaults to None.
                    If set to False it will ignore updates and only insert new records.
                insert_condition (str | bool): optional condition of the insert. Defaults to None.
                    If set to False it will ignore inserts and only update existing records.
            Raises:
                ValueError: If there are missing or new fields in the incremental data and merge_schema is set to False.
                ValueError: If the given path contains data and it is not a Delta table.
        """
        ...

    def exists(self, path: str) -> bool:
        """ Check if the data exists in the storage.

            Args:
                path (str): The path to the data.
            Returns:
                bool: True if the data exists, False otherwise.
        """
        ...

    def register_output_observers(observer: StorageOutputObserver):
        """ Register an observer to be notified when a dataframe is written to a path.

            Args:
                observer (StorageOutputObserver): The observer to register.
        """
        ...
