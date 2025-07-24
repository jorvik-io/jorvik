from jorvik.pipelines.testing import spark, smoke_test_etl

from examples.databricks.transactions.silver import nb_create_customer_summary


def test_create_customer_summary(spark):
    smoke_test_etl(nb_create_customer_summary.create_customer_summary)
