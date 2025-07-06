from jorvik.storage.isolation_providers import (_validate_isolation_context, get_spark_config,
                                                get_isolation_context_from_env_var, get_isolation_context_from_spark_config,
                                                get_no_isolation_context, get_isolation_provider)
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession
import pytest

spark = SparkSession.builder.appName("spark_no_context").getOrCreate()  # Initialize Spark session without context

def mock_spark_session(mock_get_active_session, context_config: dict, session_config: dict):
    """ Helper function to mock SparkSession.getActiveSession() with given context and session configurations.
        Because Spark context cannot be changed at runtime, this function provides a fast way
            to mock SparkSession.getActiveSession().sparkContext.getConf() and SparkSession.getActiveSession().conf
            and replace them with dictionaries that simulate the Spark configurations.
    """
    mock_context_conf = context_config
    mock_spark_context = MagicMock()
    mock_spark_context.getConf.return_value = mock_context_conf
    mock_session = MagicMock()
    mock_session.sparkContext = mock_spark_context
    mock_session.conf = session_config
    mock_get_active_session.return_value = mock_session

def test_get_spark_config_both_empty():
    ''' Test when a key is not set in neither Spark context nor session configurations'''
    with pytest.raises(ValueError):
        get_spark_config('io.jorvik.storage.isolation_provider')

def test_get_spark_config_default_value():
    ''' Test when a key is not set in neither Spark context nor session configurations but a default value is provided'''
    provider = get_spark_config('io.jorvik.storage.isolation_provider', default_value='DEFAULT_VALUE')
    assert provider == 'DEFAULT_VALUE', "Default value should be returned when key is not set in either context or session configurations."  # noqa: E501

def test_get_spark_config_session_only():
    ''' Test when a key is set in Spark session configurations but not in context configurations'''
    spark.conf.set("io.jorvik.storage.isolation_provider", 'SPARK_SESSION_ISOLATION_PROVIDER')
    provider = get_spark_config('io.jorvik.storage.isolation_provider')
    assert provider == 'SPARK_SESSION_ISOLATION_PROVIDER'

@patch('pyspark.sql.SparkSession.getActiveSession')
def test_get_spark_config_context_only(mock_get_active_session):
    ''' Test when a key is set in Spark context but not in session configurations'''
    context_config = {'io.jorvik.storage.isolation_provider': 'SPARK_CONTEXT_ISOLATION_PROVIDER'}
    mock_spark_session(mock_get_active_session, context_config, {})
    provider = get_spark_config('io.jorvik.storage.isolation_provider')
    assert provider == 'SPARK_CONTEXT_ISOLATION_PROVIDER'

@patch('pyspark.sql.SparkSession.getActiveSession')
def test_get_spark_config_both(mock_get_active_session):
    ''' Test when the same key is set in both context and session configurations'''
    context_config = {'io.jorvik.storage.isolation_provider': 'SPARK_CONTEXT_ISOLATION_PROVIDER'}
    session_config = {'io.jorvik.storage.isolation_provider': 'SPARK_SESSION_ISOLATION_PROVIDER'}
    mock_spark_session(mock_get_active_session, context_config, session_config)
    provider = get_spark_config('io.jorvik.storage.isolation_provider')
    assert provider == 'SPARK_SESSION_ISOLATION_PROVIDER', "Session configuration should take precedence over context configuration."  # noqa: E501

def test_validate_isolation_context_valid():
    """ Test that a valid isolation context does not raise an exception. """
    try:
        _validate_isolation_context("valid_context")
    except ValueError:
        pytest.fail("Valid isolation context raised ValueError unexpectedly.")

def test_validate_isolation_context_invalid():
    """ Test that an invalid isolation context raises a ValueError. """
    with pytest.raises(ValueError):
        _validate_isolation_context("/")

