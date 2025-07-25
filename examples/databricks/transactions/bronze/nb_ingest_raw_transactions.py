# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Ingestion
# MAGIC
# MAGIC This notebook creates dummy transaction data for the purposes of the example.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

from datetime import datetime

from jorvik.pipelines import etl, Input, FileOutput

from examples.databricks.transactions.bronze.schemas import raw_transactions

# COMMAND ----------

result = FileOutput(
    schema=raw_transactions.schema,
    path=raw_transactions.path,
    format=raw_transactions.format,
    mode="overwrite",
)

# COMMAND ----------

class MemoryInput(Input):
    schema = raw_transactions.schema

    def extract(self):
        return spark.createDataFrame([  # noqa: F821
            ("1", "1", "1", 1, 11.0, datetime.fromisoformat("2022-01-01T00:00:00Z")),
            ("2", "1", "2", 1, 12.0, datetime.fromisoformat("2022-01-02T00:00:00Z")),
            ("3", "1", "3", 2, 13.0, datetime.fromisoformat("2022-01-03T00:00:00Z")),
            ("4", "1", "4", 1, 14.0, datetime.fromisoformat("2022-01-04T00:00:00Z")),
            ("5", "2", "1", 3, 11.0, datetime.fromisoformat("2022-01-05T00:00:00Z")),
            ("6", "2", "1", -1, 11.0, datetime.fromisoformat("2022-01-06T00:00:00Z")),
            ("7", "2", "1", 1, 11.0, datetime.fromisoformat("2022-01-07T00:00:00Z")),
            ("8", "3", "2", 2, 12.0, datetime.fromisoformat("2022-01-08T00:00:00Z")),
            ("9", "3", "2", 2, 12.0, datetime.fromisoformat("2022-01-09T00:00:00Z")),
            ("10", "3", "2", 1, -12.0, datetime.fromisoformat("2022-01-10T00:00:00Z")),
            ("11", "4", "4", 1, 14.0, datetime.fromisoformat("2022-01-11T00:00:00Z")),
        ], schema=raw_transactions.schema)


# COMMAND ----------

@etl(inputs=MemoryInput(), outputs=result)
def ingest(df):
    return df

# COMMAND ----------

if __name__ == "__main__":
    ingest()
