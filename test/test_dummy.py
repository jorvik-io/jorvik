from fixtures.spark import *
from pyspark.sql import functions as F, SparkSession
from pyspark.testing import assertDataFrameEqual

from jorvik import dummy


def test_add_column(spark: SparkSession):
    df = spark.createDataFrame([{"id": 1}, {"id": 2}, {"id": 3}])
    expected = df.withColumn("new_column", F.lit("value")).collect()
    result = dummy.add_column(df, 'new_column', F.lit("value")).collect()

    assertDataFrameEqual(expected, result)