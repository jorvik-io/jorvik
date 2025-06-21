import pytest
from jorvik.data_lineage import node_handler
from jorvik.data_lineage.execution_plan import ExecutionNode


@pytest.mark.data_lineage
def test_extract_first_bracket_content() -> None:
    assert node_handler.extract_first_bracket_content('PreparedDeltaFileIndex [dbfs:/mnt/bronze/adventure_works/sales/data]') == 'dbfs:/mnt/bronze/adventure_works/sales/data'  # noqa: E501
    assert node_handler.extract_first_bracket_content('PreparedDeltaFileIndex [dbfs:/mnt/bronze/adventure_works/sales/data] more text [unwanted text]') == 'dbfs:/mnt/bronze/adventure_works/sales/data'  # noqa: E501
    assert node_handler.extract_first_bracket_content('No brackets here') is None


@pytest.mark.data_lineage
def test_handle_truncated_path() -> None:
    assert node_handler.handle_truncated_path('dbfs:/mnt/bronze/adventure_works/sales/data') == 'dbfs:/mnt/bronze/adventure_works/sales/data'  # noqa: E501
    assert node_handler.handle_truncated_path('/mnt/blob_storage/cleansed/sales_data/date_part=2024-07-20, ... 13 entries') == '/mnt/blob_storage/cleansed/sales_data'  # noqa: E501
    assert node_handler.handle_truncated_path('/mnt/blob_storage/cleansed/customer_info/part-00000-tid-7511665525546599716.snappy.orc, ... 12 entries') == '/mnt/blob_storage/cleansed/customer_info'  # noqa: E501


@pytest.mark.data_lineage
def test_extract_data_source() -> None:
    node = ExecutionNode(id=1, name='Scan parquet', height=1, properties={'Location': 'PreparedDeltaFileIndex [dbfs:/mnt/bronze/adventure_works/sales/data]'})  # noqa: E501
    data_source = node_handler.extract_data_source(node)
    assert data_source.scan_type == 'file_scan'
    assert data_source.location == 'dbfs:/mnt/bronze/adventure_works/sales/data'

    node = ExecutionNode(id=2, name='Scan ExistingRDD', height=1, properties={})
    data_source = node_handler.extract_data_source(node)
    assert data_source.scan_type == 'memory_scan'
    assert data_source.location == 'Scan ExistingRDD'

    node = ExecutionNode(id=3, name='Scan JDBCRelation((select * from SALES_TABLE) SPARK_GEN_SUBQ_0) [numPartitions=1]', height=1, properties={})  # noqa: E501
    data_source = node_handler.extract_data_source(node)
    assert data_source.scan_type == 'jdbc_scan'
    assert data_source.location == 'select * from SALES_TABLE'
