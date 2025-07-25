# Databricks notebook source
# MAGIC %md
# MAGIC # Customer Summary
# MAGIC
# MAGIC This notebook creates the customer_summary table. It joins the clean_transactions table with the raw_customers
# MAGIC and aggregates them on the customer level creating a summary table which every row is a different customer.

# COMMAND ----------

from jorvik.pipelines import etl, FileInput, FileOutput
from pyspark.sql import functions as F

from examples.databricks.transactions.bronze.schemas import raw_customers
from examples.databricks.transactions.silver.schemas import clean_transactions, customer_summary

# COMMAND ----------

customers = FileInput(raw_customers.path, raw_customers.format, schema=raw_customers.schema)
transactions = FileInput(clean_transactions.path, clean_transactions.format, schema=clean_transactions.schema)

summary = FileOutput(customer_summary.path, customer_summary.format, "overwrite", schema=customer_summary.schema)


# COMMAND ----------

@etl(inputs=[customers, transactions], outputs=summary)
def create_customer_summary(customers, transactions):
    customer_agg = transactions.groupBy("customer_id").agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("total_amount").alias("total_spent"),
        F.avg("total_amount").alias("avg_transaction_value"),
        F.min("transaction_date").alias("first_purchase_date"),
        F.max("transaction_date").alias("last_purchase_date")
    )

    # Join with customer data and create segments
    result = (
        customer_agg.join(customers, "customer_id", "left")
                    .withColumn("customer_segment",
                                F.when(F.col("total_spent") >= 1000, "High Value")
                                .when(F.col("total_spent") >= 500, "Medium Value")
                                .otherwise("Low Value")
                                )
        .select("customer_id", "name", "city", "total_transactions",
                "total_spent", "avg_transaction_value", "first_purchase_date",
                "last_purchase_date", "customer_segment")
        .withColumnRenamed("name", "customer_name")
        .withColumnRenamed("city", "customer_city")
    )

    return result

# COMMAND ----------

if __name__ == '__main__':
    create_customer_summary()
