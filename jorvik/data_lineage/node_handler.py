'''
This module provides functionality to handle input nodes of a Spark execution plan.
'''
from jorvik.data_lineage.execution_plan import ExecutionNode
from typing import Union, NamedTuple
import re

# cSpell: words jdbc

# Node names related to file-based scans.
FILE_BASED_SCANS = ['Scan delta', 'Scan parquet', 'Scan orc', 'Scan avro', 'Scan csv', 'Scan json',
                    'Scan text', 'Scan xml', 'Scan binaryFile']

# Node names of dataframes created by spark.createDataFrame() from pandas dataframe, inline data, etc.
MEMORY_SCANS = ['Scan ExistingRDD', 'LocalTableScan']

def extract_first_bracket_content(s: str) -> Union[str, None]:
    '''Extract the content inside the first pair of brackets in a string.'''
    match = re.search(r'\[([^\[\]]+)\]', s)
    return match.group(1) if match else None

def handle_truncated_path(path: str) -> str:
    '''
    Paths shows in file-based scans can be truncated to ...
    This typically happens when spark.read is provided with a list partitions or datafiles instead of a single path.
    Examples:
        - /mnt/blob_storage/cleansed/sales_data/date_part=2024-07-20, ... 13 entries',
        - /mnt/blob_storage/cleansed/customer_info/part-00000-tid-7511665525546599716.snappy.orc, ... 12 entries
    When this happens, shorten the path up to the up to last occurrence of '/' and before ...
    Also remove data file partitioning if any. Partition is marked by first occurrence of '='.
    Example input: /mnt/blob_storage/raw/bookings/date_part=2024-07-02/bookings_2024-07-02-T08-12-03Z.avro
    Output: /mnt/blob_storage/raw/bookings
    '''
    if re.search(r", \.\.\. \d+ entries", path):  # Path contains ', ... <integer> entries'
        p1 = path.split('=')[0]
        last_slash = p1.rfind('/')
        return p1[:last_slash]
    return path
class SparkScanDataSource(NamedTuple):
    '''
    A named tuple to represent the data source of a Spark scan.
    Attributes:
        scan_type (str): The type of scan (e.g., file_scan, memory_scan, jdbc_scan).
        location (str): The location of the data source (e.g., file path, SQL query).
    '''
    scan_type: str
    location: str

def extract_data_source(node: ExecutionNode) -> SparkScanDataSource:
    '''
    Extract the data source from the node properties.
    '''
    if node.name in FILE_BASED_SCANS:
        location_string = node.properties.get('Location')
        scan_type = 'file_scan'
        location = extract_first_bracket_content(location_string) if location_string else None
        location = handle_truncated_path(location)

    if node.name in MEMORY_SCANS:
        scan_type = 'memory_scan'
        location = node.name

    if 'Scan JDBCRelation' in node.name:
        scan_type = 'jdbc_scan'
        # Extract SQL query from the node name
        # Sample node name: Scan JDBCRelation((select * from SALES_TABLE) SPARK_GEN_SUBQ_0) [numPartitions=1]
        location = node.name.split('JDBCRelation((')[1].split(') SPARK_GEN_SUBQ')[0]

    return SparkScanDataSource(scan_type, location)
