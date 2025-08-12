# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Ingestion
# MAGIC
# MAGIC This notebook gets transaction data from sqlite database and stores as delta table.
# MAGIC This notebook demonstrates how to inherit Input class to create a DatabaseInput.
# MAGIC Run the script `/examples/sample_data_generator.py` first to generate the data.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

import pandas as pd
import sqlite3
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

class DatabaseInput(Input):
    schema = raw_transactions.schema

    def extract(self):
        db_path = '/tmp/sources/transactions_db.sqlite'
        sql = "SELECT * FROM transactions"
        with sqlite3.connect(db_path) as conn:
            transactions = pd.read_sql_query(sql, conn)
        return spark.createDataFrame(transactions)


# COMMAND ----------

@etl(inputs=DatabaseInput(), outputs=result)
def ingest(df):
    return df

# COMMAND ----------

if __name__ == "__main__":
    ingest()
