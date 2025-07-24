import pytest

from jorvik.pipelines.testing import spark, smoke_test_etl

from examples.databricks.transactions.silver import nb_clean_transactions


def test_nb_clean_transactions(spark):
    smoke_test_etl(nb_clean_transactions.clean_transactions)



if __name__ == "__main__":
    pytest.main()
