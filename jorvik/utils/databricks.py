'''This module provides utility functions to interact with Databricks built in utilities (default spark context, dbutils).'''
import json
from typing import Any

from pyspark.sql import SparkSession


def get_spark() -> SparkSession:
    return SparkSession.getActiveSession()


def get_dbutils() -> Any:
    spark = get_spark()
    if spark.conf.get("spark.databricks.service.client.enabled") == "true":
        from pyspark.dbutils import DBUtils  # type:ignore
        return DBUtils.SparkServiceClientDBUtils(spark.sparkContext)
    if spark.conf.get("spark.databricks.service.client.enabled") == "false":
        import IPython  # type: ignore
        return IPython.get_ipython().user_ns["dbutils"]  # type:ignore

def get_notebook_context():
    """ Gets the current notebook context

    Returns
    _______
        dict: notebook context
    """

    return json.loads(
        get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().toJson()  # type:ignore
    )
