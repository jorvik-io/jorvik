'''
This module contains the Observer class to track data lineage of Spark DataFrames.
'''

from typing import List
from datetime import datetime
import signal
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, TimestampType
from jorvik.data_lineage import execution_plan, node_handler
from jorvik.utils import paths

# cSpell: words signum, SIGALRM

class TimeoutException(Exception):
    def __init__(self, message):
        super().__init__(message)

def timeout_handler(signum, frame):
    raise TimeoutException("Command timed out")

class DataLineageLogger:
    '''
    Initialize the DataLineageLogger with a DataFrame and a path to store lineage logs.
    Args:
        lineage_log_path (str): The path to store lineage logs.
        timeout (int): Timeout for lineage capture in seconds. Default is 300 seconds.
    '''
    def __init__(self, lineage_log_path: str, timeout: int = 300):
        self.lineage_log_path = lineage_log_path
        self.timeout = timeout

    def _explain_dataframe(self, df: DataFrame) -> str:
        '''
        Capture the execution plan of the DataFrame using df.explain().
        To avoid blocking the main thread, a timeout is set for the explain command.
        '''
        # Register and set the alarm
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(self.timeout)

        try:
            explain_result = execution_plan.capture_explain(df)
        except TimeoutException:
            explain_result = f'ERROR. Unable to read dataframe execution plan. Timed out after {self.timeout} seconds.'
        finally:
            signal.alarm(0)  # Disable timeout alarm

        return explain_result

    def _get_data_scan_nodes(self, explain_result: StringType) -> List[execution_plan.ExecutionNode]:
        '''
        Build Spark execution plan tree and find all leaf nodes.
        '''
        exec_plan = execution_plan.build_execution_tree(explain_result)
        # If AdaptiveSparkPlan appears as a leaf, it indicates a nested execution plan. Usually seen in df.cache()
        # In this case, the real root of the nested plan is the next node after AdaptiveSparkPlan.
        scan_nodes = [f for f in exec_plan.leaves if f.name != 'AdaptiveSparkPlan']
        return scan_nodes

    def _get_data_sources(self, df: DataFrame) -> List[str]:
        '''
        Extract data sources from execution plan of the DataFrame.
        Returns a list of data sources.
        '''
        explain_result = self._explain_dataframe(df)

        if explain_result.startswith('ERROR'):
            return [explain_result]

        scan_nodes = self._get_data_scan_nodes(explain_result)
        data_sources = []
        for node in scan_nodes:
            data_source = node_handler.extract_data_source(node)
            if data_source.scan_type == 'file_scan':
                source_location = data_source.location
            else:
                source_location = f"{data_source.scan_type}: {data_source.location}"
            data_sources.append(source_location)
        return data_sources

    def _create_lineage_log(self, data_sources: List[str], output_path: str, code_file_path: str) -> DataFrame:
        '''
        Create a data lineage log entry with the output path, data sources, and observation timestamp.
        '''
        observation_ts = datetime.now()
        schema = StructType([StructField('output_path', StringType(), True),
                             StructField('data_sources', ArrayType(StringType(), True), True),
                             StructField('transform_code_file', StringType(), True),
                             StructField('observation_ts', TimestampType(), True)
                             ])
        spark = SparkSession.getActiveSession()
        return spark.createDataFrame(data=[[output_path, data_sources, code_file_path, observation_ts]], schema=schema)

    def _store_lineage_log(self, lineage_log: DataFrame) -> None:
        '''Store lineage log entry to log path.'''
        from jorvik import storage  # Lazy import to avoid circular dependency
        st = storage.configure(track_lineage=False)
        st.write(lineage_log, self.lineage_log_path, format="delta", mode="append")

    def update(self, df: DataFrame, output_path: str) -> None:
        data_sources = self._get_data_sources(df)
        code_file_path = paths.get_codefile_path()
        lineage_log = self._create_lineage_log(data_sources, output_path, code_file_path)
        self._store_lineage_log(lineage_log)
