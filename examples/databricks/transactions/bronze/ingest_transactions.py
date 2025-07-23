# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Ingestion
# MAGIC
# MAGIC This notebook creates dummy transaction data for the purposes of the example, in a realistic scenario this notebook would fetch data from a production system.
# MAGIC
# MAGIC For example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.

# COMMAND ----------

from datetime import datetime

from pyspark.sql.types import StringType, StructField, StructType, IntegerType, FloatType, TimestampType
from pyspark.sql import functions as F

from jorvik.pipelines import etl, Input, FileOutput
from jorvik.storage import configure

# COMMAND ----------

raw_transactions = FileOutput(
    schema=StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("quantity", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("timestamp", TimestampType(), True)
    ]),
    path="/mnt/bronze/raw_transactions/data",
    format="delta",
    mode="overwrite",
)

# COMMAND ----------

class MemoryInput(Input):
    schema = raw_transactions.schema

    def extract(self):
        return spark.createDataFrame([
            ("1", "1", "1", 1, 11.0, datetime.fromisoformat("2022-01-01T00:00:00Z")),
            ("2", "1", "2", 2, 12.0, datetime.fromisoformat("2022-01-02T00:00:00Z")),
            ("3", "1", "3", 3, 13.0, datetime.fromisoformat("2022-01-03T00:00:00Z")),
            ("4", "1", "4", 4, 14.0, datetime.fromisoformat("2022-01-04T00:00:00Z")),
            ("5", "2", "1", 5, 11.0, datetime.fromisoformat("2022-01-05T00:00:00Z")),
            ("6", "2", "1", 6, 11.0, datetime.fromisoformat("2022-01-06T00:00:00Z")),
            ("7", "2", "1", 7, 11.0, datetime.fromisoformat("2022-01-07T00:00:00Z")),
            ("8", "3", "2", 8, 12.0, datetime.fromisoformat("2022-01-08T00:00:00Z")),
            ("9", "3", "2", 9, 12.0, datetime.fromisoformat("2022-01-09T00:00:00Z")),
            ("10", "3", "2", 10, 12.0, datetime.fromisoformat("2022-01-10T00:00:00Z")),
            ("11", "4", "4", 11, 14.0, datetime.fromisoformat("2022-01-11T00:00:00Z")),
        ], schema=raw_transactions.schema)


# COMMAND ----------

@etl(inputs=MemoryInput(), outputs=raw_transactions)
def ingest_transactions(raw_transactions):
    return raw_transactions

# COMMAND ----------

if __name__ == "__main__":
    ingest_transactions()
