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
        raise ValueError(f"Invalid isolation context name {context}. This name is not accepted as a directory in the filesystem.")  # noqa: E501

def get_spark_config(config_key: str, default_value: str = None) -> str:
    """ Get configuration from Spark session or Spark context.
        If the same configuration is available in both, the Spark session configuration takes precedence.

        Returns:
            config_key: The Spark configuration as a string.
        Raises:
            ValueError: If the configuration key is not found in either Spark session or Spark context.
    """
    context_config = SparkSession.getActiveSession().sparkContext.getConf().get(config_key, None)
    session_config = SparkSession.getActiveSession().conf.get(config_key, None)
    if session_config:
        return session_config
    if context_config:
        return context_config
    if default_value:
        return default_value
    raise ValueError(f"Configuration key '{config_key}' not found in either Spark session or Spark context.")

def get_isolation_context_from_env_var() -> str:
    """ Get the isolation context from the environment variable.

        Returns:
            str: The isolation context as a string.
        Raises:
            ValueError: If the environment variable 'JORVIK_ISOLATION_CONTEXT' is not set.
    """
    context = os.environ.get("JORVIK_ISOLATION_CONTEXT")
    if context is None:
        raise ValueError("Environment variable 'JORVIK_ISOLATION_CONTEXT' is not set.")
    return context

def get_isolation_context_from_spark_config() -> str:
    """ Get the isolation context from the Spark configuration.

        Returns:
            str: The isolation context as a string.
    """
    return get_spark_config("io.jorvik.storage.isolation_context")

def get_no_isolation_context() -> str:
    """ Get an empty isolation context, used for non-isolated storage scenarios

        Returns:
            str: An empty string indicating no isolation context.
    """
    return ""

def get_isolation_provider() -> Callable:
    """ Get the isolation provider for the current Spark session.

        Returns:
            Callable: A function that returns isolation context as a string.
    """
    provider_config = get_spark_config("io.jorvik.storage.isolation_provider", default_value="NO_ISOLATION")

    PROVIDERS = {
        'NO_ISOLATION': get_no_isolation_context,
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
    if provider_config != 'NO_ISOLATION':
        _validate_isolation_context(provider())
    return provider
