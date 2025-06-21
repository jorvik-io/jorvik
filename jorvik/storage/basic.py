from jorvik.storage.protocols import StorageOutputObserver
from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery
from pyspark.errors import AnalysisException


class BasicStorage():
    def __init__(self):
        """ Initialize the BasicStorage class. """
        self.output_observers = []

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
        if format not in ["delta", "parquet", "json", "csv", "orc"]:
            raise ValueError(f"Unsupported format: {format}")
        spark = SparkSession.getActiveSession()
        options = {} if not options else options

        if format == "csv":
            options.setdefault("header", True)
            options.setdefault("inferSchema", True)

        reader = spark.read.format(format)
        reader = reader.options(**options)
        return reader.load(path)

    def readStream(self, path: str, format: str, options: dict = None) -> DataFrame:
        """ Stream data from the storage.

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
        if format not in ["delta", "parquet", "json", "csv", "orc"]:
            raise ValueError(f"Unsupported format: {format}")
        spark = SparkSession.getActiveSession()

        if format == "delta":
            reader = spark.readStream.format(format)
        else:
            schema = self.read(path, format, options).schema
            reader = spark.readStream.schema(schema).format(format)
        if options:
            reader = reader.options(**options)
        return reader.load(path)

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
        if format not in ["delta", "parquet", "json", "csv", "orc"]:
            raise ValueError(f"Unsupported format: {format}")

        options = {} if not options else options
        if format == "csv":
            options.setdefault("header", True)
            options.setdefault("delimiter", ",")

        writer = df.write.format(format)
        if mode == "overwrite":
            writer = writer.option("overwriteSchema", "true")
        if partition_fields:
            writer = writer.partitionBy(partition_fields)

        writer = writer.options(**options)
        writer.mode(mode).save(path)
        self.notify_output_observers(df, path)

    def writeStream(self, df: DataFrame, path: str, format: str, checkpoint: str,
                    partition_fields: str | list = "", options: dict = None) -> StreamingQuery:
        """ Stream data to the storage.

            Args:
                df (DataFrame): The DataFrame to write.
                path (str): The path to write the data to. Available formats are:
                    - delta
                    - parquet
                    - json
                    - csv
                    - orc
                format (str): The format of the data.
                checkpoint (str): The checkpoint location.
                partition_fields (str | list): The fields to partition by.
                options (dict): Additional options for writing. Default is None.
        """
        if format not in ["delta", "parquet", "json", "csv", "orc"]:
            raise ValueError(f"Unsupported format: {format}")

        self.notify_output_observers(df, path)

        writer = df.writeStream.format(format)
        if partition_fields:
            writer = writer.partitionBy(partition_fields)
        if options:
            writer = writer.options(**options)

        return writer.option("checkpointLocation", checkpoint).start(path)

    def exists(self, path: str) -> bool:
        """ Check if the path exists.

            Args:
                path (str): The path to check.
            Returns:
                bool: True if the path exists, False otherwise.
        """
        spark = SparkSession.getActiveSession()

        # Check if the path is a Delta table as this is a lot faster check.
        if DeltaTable.isDeltaTable(spark, path):
            return True

        # Check with the Java FileSystem API
        # This is a fast check but it relies in pyspark's internals.
        try:
            sc = spark.sparkContext
            fs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
            return fs.exists(sc._jvm.org.apache.hadoop.fs.Path(path))
        except Exception:
            pass

        # Try to read the path as a DataFrame
        # This is a slower check thus it serves as a last resort.
        try:
            spark.read.format("text").load(path)
            return True
        except AnalysisException as e:
            if "Path does not exist" in str(e):
                return False
            else:
                raise e

    def register_output_observer(self, observer: StorageOutputObserver) -> None:
        """ Register an observer to be notified when a dataframe is written to a path.

            Args:
                observer (StorageOutputObserver): The observer to register.
        """
        self.output_observers.append(observer)

    def notify_output_observers(self, df: DataFrame, output_path: str) -> None:
        """ Notify all registered observers when a dataframe is written to a path. """
        for observer in self.output_observers:
            observer.update(df, output_path)
