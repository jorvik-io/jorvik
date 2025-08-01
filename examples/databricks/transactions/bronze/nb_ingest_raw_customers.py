# Databricks notebook source
# MAGIC %md
# MAGIC # Customers Ingestion
# MAGIC
# MAGIC This notebook creates dummy customer data for the purposes of the example.
# MAGIC In a realistic scenario this notebook would fetch data from a production system for example:
# MAGIC - It could listen to an event topic and accumulate data in a Delta Table.
# MAGIC - It could copy data from a transactional Database.
# MAGIC - It could fetch data from the API of the ERP system.
# MAGIC

# COMMAND ----------

from datetime import date

from jorvik.pipelines import etl, Input, FileOutput

from examples.databricks.transactions.bronze.schemas import raw_customers

# COMMAND ----------

result = FileOutput(
    schema=raw_customers.schema,
    path=raw_customers.path,
    format=raw_customers.format,
    mode="overwrite",
)

# COMMAND ----------

class MemoryInput(Input):
    schema = raw_customers.schema

    def extract(self):
        return spark.createDataFrame([  # noqa: F821
            ("1", "John Doe", "jhon.doe@mail.com", 30, "New York", date(2022, 1, 1)),
            ("2", "Jane Doe", "jane.doe@mail.com", 25, "Los Angeles", date(2022, 1, 1)),
            ("3", "Mike Smith", "mike.smith@mail.com", 40, "Chicago", date(2022, 1, 1)),
            ("4", "Sara Johnson", "sara.johnson@mail.com", 35, "Houston", date(2022, 1, 1)),
            ("5", "Tom Brown", "tom.brown@mail.com", 28, "Miami", date(2022, 1, 1)),
        ], schema=raw_customers.schema)


# COMMAND ----------

@etl(inputs=MemoryInput(), outputs=result)
def ingest(df):
    return df

# COMMAND ----------

if __name__ == "__main__":
    ingest()
