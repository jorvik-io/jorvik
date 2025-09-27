# Databricks notebook source
# MAGIC %md
# MAGIC # Transactions Ingestion
# MAGIC
# MAGIC This notebook gets transaction data from sqlite database and stores as delta table.
# MAGIC This notebook demonstrates how to inherit Input class to create a DatabaseInput.
# MAGIC Sample data is generated to DBFS storage to simulate a production source.
# MAGIC Modify data volume and location in `examples.sample_data_generator` as needed.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

import pandas as pd
import sqlite3
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import IntegerType, FloatType
from jorvik.pipelines import etl, Input, FileOutput
from jorvik.utils import databricks
from examples.databricks.transactions.bronze.schemas import raw_transactions
from examples.sample_data_generator import generate_transactions, save_sqlite_to_dbfs

# COMMAND ----------

result = FileOutput(
    schema=raw_transactions.schema,
    path=raw_transactions.path,
    format=raw_transactions.format,
    mode="overwrite",
)

# COMMAND ----------

def cast_data_types(df: DataFrame) -> DataFrame:
    return (df
            .withColumn("quantity", F.col("quantity").cast(IntegerType()))
            .withColumn("price", F.col("price").cast(FloatType()))
            )

class DatabaseInput(Input):
    schema = raw_transactions.schema

    def extract(self):
        db_path = '/dbfs/tmp/sources/transactions_db.sqlite'
        sql = "SELECT * FROM transactions"
        with sqlite3.connect(db_path) as conn:
            transactions = pd.read_sql_query(sql, conn, parse_dates=['timestamp'])
        transactions = databricks.get_spark().createDataFrame(transactions)
        return cast_data_types(transactions)

# COMMAND ----------

@etl(inputs=DatabaseInput(), outputs=result)
def ingest(df):
    return df

# COMMAND ----------

if __name__ == "__main__":
    save_sqlite_to_dbfs(generate_transactions(), 'transactions_db')
    ingest()
