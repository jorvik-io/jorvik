from jorvik.pipelines.testing import spark, smoke_test_etl  # noqa: F401

from examples.databricks.transactions.silver import nb_clean_transactions


def test_nb_clean_transactions(spark):  # noqa: F811
    smoke_test_etl(nb_clean_transactions.clean)
