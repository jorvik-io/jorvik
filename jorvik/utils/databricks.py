'''This module provides utility functions to interact with Databricks built in utilities (default spark context, dbutils).'''
import json
from typing import Any

from pyspark.sql import SparkSession


class DatabricksUtilsError(Exception):
    """Custom exception for Databricks utility errors."""

    def __init__(self, error: str = None):
        message = f"{error}. Ensure you are running this code in a Databricks notebook environment."  # noqa: E501
        super().__init__(message)

def get_spark() -> SparkSession:
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise DatabricksUtilsError("No active Spark session found")
    return spark

def get_dbutils() -> Any:
    """ Gets the Databricks dbutils client
    Returns:
        DBUtils: dbutils client
    Raises:
        DatabricksUtilsError: If the dbutils client configuration cannot be determined.
    """
    spark = get_spark()
    client_config = spark.conf.get("spark.databricks.service.client.enabled", None)

    try:
        if client_config == "true":
            from pyspark.dbutils import DBUtils  # type:ignore
            return DBUtils.SparkServiceClientDBUtils(spark.sparkContext)
        else:
            import IPython  # type: ignore
            return IPython.get_ipython().user_ns["dbutils"]  # type:ignore
    except Exception:
        raise DatabricksUtilsError("Could not determine the dbutils client configuration")

def get_notebook_context() -> dict:
    """ Gets the current notebook context

    Returns:
        dict: notebook context
    """

    return json.loads(
        get_dbutils().notebook.entry_point.getDbutils().notebook().getContext().toJson()  # type:ignore
    )

def get_active_branch() -> str:
    """ Returns the current active Git branch, if applicable.
        For production workflows running from a Workspace (non-Git) folder, an empty string
        is returned instead of a branch name. This indicates that no isolation should be applied.

    Returns:
        str: active_branch or ''
    """

    context = get_notebook_context()

    if "mlflowGitReference" not in context["extraContext"]:
        return ""

    branch = context["extraContext"]["mlflowGitReference"]
    return branch

def get_current_user() -> str:
    """ Gets the current Databricks user"""
    return get_notebook_context()['tags']['user']

def get_cluster_id() -> str:
    """ Gets the current Databricks cluster ID"""
    return get_notebook_context()['tags']['clusterId']

def get_notebook_path() -> str:
    """ Gets the current notebook path"""
    return get_notebook_context()["extraContext"]["notebook_path"]
