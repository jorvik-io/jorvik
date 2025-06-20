# Pipelines
The pipelines package offers utilities from composing ETL pipelines that are easily testable robust and reliable. 

## Build
The package binds together data paths with schema definitions with the following classes:
FileInput, FileOutput, StreamFileInput, StreamFileOutput. FileInputs need to be used together with FileOutputs and StreamFileInputs with StreamFileOutputs. 

Here is an example of a simple joining of two tables ETL job.
```python
    first = FileInput(schema=StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
    ]), path="/mnt/s/simple_join/first", format="delta")

    second = FileInput(schema=StructType([
        StructField("id", IntegerType(), True),
        StructField("another_name", StringType(), True),
        StructField("another_value", StringType(), True),
    ]), path="/mnt/s/simple_join/second", format="delta")

    out = FileOutput(schema=StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", StringType(), True),
        StructField("another_name", StringType(), True),
        StructField("another_value", StringType(), True),
    ]), path="/mnt/s/simple_join/out", format="delta", mode="overwrite")

    @etl(inputs=[first, second], outputs=[out])
    def simple_join(first, second):
        return first.join(second, on=["id"], how="inner")

    simple_join()
```


Of course not all data live in Files sometimes we need to bring data from other sources.

You can achieve this by creating a custom class that extends the Input and Output classes and override the extract and load functions respectively.

Here is an example for reading data from an API.

```python
class APIInput(Input):
    schema = StructType(...)

    def extract(self):
        spark = SparkSession.getActiveSession()
        data = requests.get('http://mydata.com').json()
        return spark.createDataFrame(data)
```

## Test
**_NOTE:_** For testing the tests extra requirements are necessary. You only need these on your CICD pipeline and development environment, avoid installing them in prod. To install them use `pip install jorvik[tests]`

To test a pipeline and verify that with the input schema definitions the output schema definitions will be produced you simply need to call the smoke_test_etl function

```python
from jorvik.pipelines.testing import smoke_test_etl, spark


def test_simple_join(spark: SparkSession):
    smoke_test_etl(simple_join)

```
