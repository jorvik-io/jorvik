from pytest_mock import MockerFixture

from jorvik import storage
from jorvik.storage import BasicStorage
from jorvik.storage.isolation import IsolatedStorage


def test_configure_with_isolation_provider(mocker: MockerFixture):
    """ Setting Isolation Provider should be enough to configure Isolated Storage. """
    mocker.patch("jorvik.storage.BasicStorage.exists")
    mocker.patch("jorvik.storage.SparkSession")
    spark = mocker.patch("jorvik.storage.isolation.SparkSession")
    spark.getActiveSession().conf = {}

    st = storage.configure(lambda: "my_feature")
    assert st._create_isolation_path("/mnt/my_table/data") == "/mnt/isolation/my_feature/my_table/data"


def test_configure_no_arguments_no_config(mocker: MockerFixture):
    mocker.patch("jorvik.storage.SparkSession")
    mocker.patch("jorvik.storage.isolation_providers.get_spark_config", return_value="NO_ISOLATION")
    st = storage.configure()
    assert isinstance(st, BasicStorage)


def test_configure_with_config(mocker: MockerFixture):
    mocker.patch("jorvik.storage.SparkSession")
    mocker.patch("jorvik.storage.isolation_providers.get_spark_config", return_value="SPARK_CONFIG")
    st = storage.configure()
    assert isinstance(st, IsolatedStorage)


def test_configure_with_track_lineage(mocker: MockerFixture):
    mocker.patch("jorvik.storage.SparkSession")
    mocker.patch("jorvik.storage.isolation_providers.get_spark_config", return_value="NO_ISOLATION")
    st = storage.configure(track_lineage=True)
    assert len(st.output_observers) == 1
