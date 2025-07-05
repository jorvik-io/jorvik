"""This module provides functions to manage isolation contexts for Jorvik IsolatedStorage."""

import os
import tempfile
from typing import Callable
from pyspark.sql import SparkSession
from jorvik.utils import databricks, git

def _validate_isolation_context(context: str) -> None:
    """ Validate the isolation context to ensure it is a valid directory name.
        Raises ValueError if the context is not a valid identifier.

        Args:
            context (str): The isolation context to validate.
        Raises:
            ValueError: If the context is not a valid identifier.
    """
    try:
        with tempfile.TemporaryDirectory() as tmp:
            test_path = os.path.join(tmp, context)
            os.mkdir(test_path)
        return True
    except (OSError, ValueError):
        return ValueError(f"Invalid isolation context name {context}. This name is not accepted as a directory in the filesystem.")  # noqa: E501

def get_isolation_context_from_env_var() -> str:
    """ Get the isolation context from the environment variable.

        Returns:
            str: The isolation context as a string.
    """
    return os.environ.get("JORVIK_ISOLATION_CONTEXT", "")

def get_isolation_context_from_spark_config() -> str:
    """ Get the isolation context from the Spark configuration.

        Returns:
            str: The isolation context as a string.
    """
    return SparkSession.getActiveSession().sparkContext.getConf().get("io.jorvik.storage.isolation_context", "")

def get_isolation_provider() -> Callable:
    """ Get the isolation provider for the current Spark session.

        Returns:
            Callable: A function that returns isolation context as a string.
    """
    provider_config = SparkSession.getActiveSession().sparkContext.getConf().get("io.jorvik.storage.isolation_provider", "")

    PROVIDERS = {
        'DATABRICKS_GIT_BRANCH': databricks.get_active_branch,
        'DATABRICKS_USER': databricks.get_current_user,
        'DATABRICKS_CLUSTER': databricks.get_cluster_id,
        'GIT_BRANCH': git.get_current_git_branch,
        'ENVIRONMENT_VARIABLE': get_isolation_context_from_env_var,
        'SPARK_CONFIG': get_isolation_context_from_spark_config
    }

    try:
        provider = PROVIDERS[provider_config]
    except KeyError:
        raise ValueError(f"Unknown isolation provider: {provider_config}. Supported providers are: {list(PROVIDERS.keys())}.")  # noqa: E501
    _validate_isolation_context(provider())
    return provider
