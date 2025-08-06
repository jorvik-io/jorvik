from pyspark.sql import SparkSession

from typing import Optional, Callable, Union

from jorvik.storage.basic import BasicStorage
from jorvik.storage.isolation_providers import get_isolation_provider
from jorvik.storage.isolation import IsolatedStorage
from jorvik.data_lineage.observer import DataLineageLogger
from jorvik.storage.protocols import Storage


def configure(
    isolation_provider: Optional[Callable[[], str]] = None,
    verbose: bool = False,
    track_lineage: bool = True
) -> Union[BasicStorage, IsolatedStorage]:
    """
    Configure the storage system, optionally wrapping it with IsolatedStorage.

    Args:
        isolation_provider (Callable): A function that returns an isolation context (e.g., "branch name").
        verbose (bool): Enable verbose logging.
        track_lineage (bool): Attach the DataLineageLogger if true.

    Returns:
        BasicStorage or IsolatedStorage
    """
    st = BasicStorage()
    conf = SparkSession.getActiveSession().sparkContext.getConf()
    lineage_log_path = conf.get('io.jorvik.data_lineage.log_path', '')
    production_context = conf.get('io.jorvik.storage.production_context', 'main,master,production,prod').split(',')

    if track_lineage and lineage_log_path:
        st.register_output_observer(DataLineageLogger(lineage_log_path))

    # If there's a isolation provider, check the isolation value to configure IsolatedStorage.
    if isolation_provider is None:
        isolation_provider = get_isolation_provider()

    isolation = isolation_provider()
    if isolation and isolation.lower() not in [p.strip().lower() for p in production_context]:
        return IsolatedStorage(st, verbose=verbose, isolation_provider=isolation_provider)

    return st

__all__ = ["Storage", "configure"]
