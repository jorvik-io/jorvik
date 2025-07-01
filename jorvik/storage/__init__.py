from typing import Protocol
import re
import os

from pyspark.sql import DataFrame
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql import SparkSession

from jorvik.storage.basic import BasicStorage
from jorvik.data_lineage.observer import DataLineageLogger
from jorvik.utils import databricks, git


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

def _sanitize_isolation_context(context: str) -> str:
    """ Sanitize the isolation context to ensure it is a valid identifier.
        Replace all non-alphanumeric characters and underscores with an underscore.

        Args:
            context (str): The isolation context to sanitize.
        Returns:
            str: The sanitized isolation context.
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', context)

def get_isolation_context() -> str:
    """ Get the isolation context for the current Spark session.

        Returns:
            str: The isolation context.
    """
    provider = SparkSession.getActiveSession().sparkContext.getConf().get("io.jorvik.storage.isolation_provider", "")

    if provider == 'DATABRICKS_GIT_BRANCH':
        context = databricks.get_active_branch()
    elif provider == 'DATABRICKS_USER':
        context = databricks.get_current_user()
    elif provider == 'DATABRICKS_CLUSTER':
        context = databricks.get_cluster_id()
    elif provider == 'GIT_BRANCH':
        context = git.get_current_git_branch()
    elif provider == 'ENVIRONMENT_VARIABLE':
        context = os.environ.get("JORVIK_ISOLATION_CONTEXT", "")
    elif provider == 'SPARK_CONFIG':
        context = SparkSession.getActiveSession().sparkContext.getConf().get("io.jorvik.storage.isolation_context", "")
    else:
        raise ValueError(f"Unknown isolation provider: {provider}. Supported providers are: 'DATABRICKS_GIT_BRANCH', 'DATABRICKS_USER', 'DATABRICKS_CLUSTER', 'GIT_BRANCH', 'ENVIRONMENT_VARIABLE', 'SPARK_CONFIG'.")  # noqa: E501

    return _sanitize_isolation_context(context)

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
