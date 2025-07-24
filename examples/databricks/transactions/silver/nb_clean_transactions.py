# Databricks notebook source
# MAGIC %md
# MAGIC # Clean Transactions
# MAGIC
# MAGIC This notebook cleans transaction data by removing invalid transactions
# MAGIC and computing a few extra columns like 'total_amount'. A transaction is considered invalid if:
# MAGIC - The amount is negative.
# MAGIC - The quantity is negative.
# MAGIC
# MAGIC

# COMMAND ----------

from jorvik.pipelines import FileInput, FileOutput, etl
from pyspark.sql import functions as F

from examples.databricks.transactions.bronze.schemas import raw_transactions
from examples.databricks.transactions.silver.schemas import clean_transactions

# COMMAND ----------

raw = FileInput(
    path=raw_transactions.path,
    schema=raw_transactions.schema,
    format=raw_transactions.format
)

clean = FileOutput(
    path=clean_transactions.path,
    schema=clean_transactions.schema,
    format=clean_transactions.format,
    mode='overwrite'
)

# COMMAND ----------

@etl(raw, clean)
def clean(raw):
    return (
        raw.filter("quantity > 0")
        .filter("price > 0")
        .withColumn("total_amount", (F.col("quantity") * F.col("price")).cast('float'))
        .withColumn("transaction_date", F.to_date("timestamp"))
        .withColumn("transaction_hour", F.hour("timestamp"))
        .withColumn("unit_price", F.col("price"))
        .select("transaction_id", "customer_id", "product_id",
                "quantity", "unit_price", "total_amount",
                "transaction_date", "transaction_hour")
    )

# COMMAND ----------

if __name__ == '__main__':
    clean()
