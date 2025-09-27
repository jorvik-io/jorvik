# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Ingestion
# MAGIC
# MAGIC This notebook gets customer data from file input and stores as delta table.
# MAGIC Sample data is generated to DBFS storage to simulate a production source.
# MAGIC Modify data volume and location in `examples.sample_data_generator` as needed.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

from jorvik.pipelines import etl, FileInput, FileOutput
from examples.databricks.transactions.bronze.schemas import raw_customers
from examples.sample_data_generator import generate_customers, save_csv_to_dbfs

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
    save_csv_to_dbfs(generate_customers(), 'customers')
    ingest()
