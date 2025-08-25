# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Ingestion
# MAGIC
# MAGIC This notebook gets customer data from file input and stores as delta table.
# MAGIC Run the script `/examples/sample_data_generator.py` first to generate the data.
# MAGIC Modify the data volume and location as needed.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

from jorvik.pipelines import etl, FileInput, FileOutput
from examples.databricks.transactions.bronze.schemas import raw_customers

# COMMAND ----------

result = FileOutput(
    schema=raw_customers.schema,
    path=raw_customers.path,
    format=raw_customers.format,
    mode="overwrite",
)

# COMMAND ----------
input = FileInput(
    path="/tmp/sources/customers.csv",
    format="csv",
    schema=raw_customers.schema,
)

# COMMAND ----------

@etl(inputs=input, outputs=result)
def ingest(df):
    return df

# COMMAND ----------

if __name__ == "__main__":
    ingest()
