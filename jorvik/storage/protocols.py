from typing import Protocol
from pyspark.sql import DataFrame

class StorageOutputObserver(Protocol):
    def update(self, df: DataFrame, output_path: str) -> None:
        """ Action when Storage subject notifies an event of writing a DataFrame to a path.
            Args:
                df (DataFrame): The DataFrame to write.
                output_path (str): The path to write the data to.
        """
        ...
