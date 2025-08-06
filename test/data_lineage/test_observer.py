from jorvik.data_lineage import observer
from fixtures.data_lineage import spark, SparkSession, mock_capture_explain  # noqa: F401 F403
import pytest
from pytest_mock import MockerFixture
from unittest.mock import patch
import time


def mock_hanging_explain(*args, **kwargs) -> str:
    time.sleep(3)  # Simulate a long-running explain command


@pytest.mark.data_lineage
def test_get_data_sources_success(spark: SparkSession, mock_capture_explain: MockerFixture) -> None:  # noqa: F811
    df = spark.createDataFrame([(1, "test")], ["id", "value"])
    logger = observer.DataLineageLogger(lineage_log_path="/path/to/logs")
    data_sources = logger._get_data_sources(df)
    assert set(data_sources) == {
        'dbfs:/mnt/bronze/adventure_works/sales/data',
        'memory_scan: Scan ExistingRDD'
    }


@pytest.mark.data_lineage
def test_get_data_sources_timeout(spark: SparkSession) -> None:  # noqa: F811
    df = spark.createDataFrame([(1, "test")], ["id", "value"])

    # Simulate a hanging explain command
    with patch('jorvik.data_lineage.execution_plan.capture_explain', side_effect=mock_hanging_explain):
        logger = observer.DataLineageLogger(lineage_log_path="/path/to/logs", timeout=1)
        data_sources = logger._get_data_sources(df)
        assert data_sources == ['ERROR. Unable to read dataframe execution plan. Timed out after 1 seconds.']


@pytest.mark.data_lineage
def test_create_lineage_log(spark: SparkSession) -> None:
    lineage_log_path = "/path/to/logs"
    logger = observer.DataLineageLogger(lineage_log_path=lineage_log_path)

    output_path = "/path/to/output"
    data_sources = [
        'dbfs:/mnt/bronze/adventure_works/sales/data',
        'memory_scan: Scan ExistingRDD'
    ]
    code_file_path = "/path/to/codefile.py"

    lineage_log = logger._create_lineage_log(data_sources, output_path, code_file_path)
    assert set(lineage_log.columns) == {'output_path', 'data_sources', 'transform_code_file', 'observation_ts'}
