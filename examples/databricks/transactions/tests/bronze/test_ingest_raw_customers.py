from jorvik.pipelines.testing import spark, smoke_test_etl  # noqa: F401

from examples.databricks.transactions.bronze import nb_ingest_raw_customers


def test_nb_clean_transactions(spark):  # noqa: F811
    smoke_test_etl(nb_ingest_raw_customers.ingest)
