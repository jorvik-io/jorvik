import pytest
from jorvik import storage
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, TimestampType

from fixtures.data_lineage import spark, LINEAGE_LOG_PATH  # noqa: F401 F403


LINEAGE_LOG_SCHEMA = StructType([StructField('output_path', StringType(), True),
                                 StructField('data_sources', ArrayType(StringType(), True), True),
                                 StructField('observation_ts', TimestampType(), True)])


@pytest.mark.data_lineage
def test_write_lineage_log(spark: SparkSession):  # noqa: F811
    """ Test that lineage log entry is created when writing to storage. """

    df = spark.createDataFrame([
        {"id": 1, "name": "Alice", "value": "1.0"},
        {"id": 2, "name": "Bob", "value": "2.0"},
        {"id": 3, "name": "Cathy", "value": "3.0"},
    ])

    log_path = spark.sparkContext.getConf().get("io.jorvik.data_lineage.log_path", "NOT FOUND IN SPARK CONTEXT")
    assert log_path == LINEAGE_LOG_PATH, "Lineage log path is not set correctly."

    st = storage.configure()
    output_path = "/tmp/sample_data"
    st.write(df, output_path, format="delta", mode="overwrite")
    lineage_log = st.read(LINEAGE_LOG_PATH, format="delta")

    assert lineage_log.schema == LINEAGE_LOG_SCHEMA, "Lineage log schema does not match expected schema."

    output_path_log = lineage_log.select("output_path").collect()[0][0]
    assert output_path_log == output_path, "Output path in lineage log does not match expected output path"

    data_source_log = lineage_log.select("data_sources").collect()[0][0][0]
    assert data_source_log == "memory_scan: Scan ExistingRDD", "Data source in lineage log does not match expected data source"
