from delta import DeltaTable
from pyspark.errors import AnalysisException
from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from jorvik.audit import schemas
from jorvik.storage.protocols import StorageOutputObserver


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
                ValueError: If both insert_condition and update_condition are set to False.
        """
        if isinstance(update_condition, bool):
            update_condition = str(update_condition).lower()

        if isinstance(insert_condition, bool):
            insert_condition = str(insert_condition).lower()

        if insert_condition == update_condition == 'false':
            raise ValueError("Both inserts and updates are ignored this operation will not have an effect.")

        if not self.exists(path):
            self.write(df, path, format='delta', mode='overwrite', partition_fields=partition_fields)
            return

        spark = SparkSession.getActiveSession()

        if not DeltaTable.isDeltaTable(spark, path):
            raise ValueError("The given path is not a Delta Table.")

        df = self._merge_schema(df, path, merge_schemas)

        delta_table_full = DeltaTable.forPath(spark, path)

        (
            delta_table_full.alias('full')
                            .merge(df.alias('incremental'), merge_condition)
                            .whenMatchedUpdateAll(update_condition)
                            .whenNotMatchedInsertAll(insert_condition)
                            .execute()
        )

        self.notify_output_observers(df, path)

    def _merge_schema(self, df: DataFrame, path: str, merge_schemas: bool) -> tuple[DataFrame, DataFrame]:
        current_table = self.read(path, format='delta')
        if schemas.are_equal(df.schema, current_table.schema):
            return df

        current_table_fields_names = {f.name for f in current_table.schema}
        new_fields_names = {f.name for f in df.schema}

        new = [f for f in df.schema if f.name not in current_table_fields_names and f.name]
        missing = [f for f in current_table.schema if f.name not in new_fields_names and f.name]

        if not merge_schemas:
            raise ValueError(f"""Incremental data have a different schema.
                             New fields: {new}
                             Missing fields: {missing}
                             """)

        for f in missing:
            df = df.withColumn(f.name, F.lit(None).cast(f.dataType))

        for f in new:
            current_table = current_table.withColumn(f.name, F.lit(None).cast(f.dataType))

        if new:
            self.write(current_table, path, format='delta', mode='overwrite',
                       options={"mergeSchema": "true", "replaceWhere": "true"})

        return df

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
