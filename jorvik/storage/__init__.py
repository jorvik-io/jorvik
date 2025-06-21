from typing import Protocol

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import SparkSession

from jorvik.storage.basic import BasicStorage
from jorvik.data_lineage.observer import DataLineageLogger


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

    def exists(self, path: str) -> bool:
        """ Check if the data exists in the storage.

            Args:
                path (str): The path to the data.
            Returns:
                bool: True if the data exists, False otherwise.
        """
        ...


def configure(track_lineage: bool = True) -> Storage:
    """ Configure the storage.
        Args:
            track_lineage (bool): Whether to track data lineage. Default is True.
        Returns:
            Storage: The configured storage instance.
    """
    st = BasicStorage()
    conf = SparkSession.getActiveSession().sparkContext.getConf()
    lineage_log_path = conf.get('io.jorvik.data_lineage.log_path', '')
    if track_lineage and lineage_log_path:
        st.register_output_observer(DataLineageLogger(lineage_log_path))
    return st


__all__ = ["Storage", "configure"]
