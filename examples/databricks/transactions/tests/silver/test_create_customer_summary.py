from jorvik.pipelines.testing import spark, smoke_test_etl  # noqa: F401

from examples.databricks.transactions.silver import nb_create_customer_summary


def test_create_customer_summary(spark):  # noqa: F811
    smoke_test_etl(nb_create_customer_summary.create_customer_summary)
