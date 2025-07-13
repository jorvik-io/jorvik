from pyspark.sql import SparkSession

from typing import Optional, Callable, Union, List

from jorvik.storage.basic import BasicStorage
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
    production_context = conf.get('io.jorvik.storage.production_context') or ['main', 'master', 'production', 'prod']
    production_context = _normalize_contexts(production_context)

    if track_lineage and lineage_log_path:
        st.register_output_observer(DataLineageLogger(lineage_log_path))

    # If there's a isolation provider, check the isolation value to configure IsolatedStorage.
    if isolation_provider:
        isolation = isolation_provider()
        if isolation and isolation.lower() not in production_context:
            return IsolatedStorage(st, verbose=verbose, isolation_provider=isolation_provider)

    return st

__all__ = ["Storage", "configure"]


def _normalize_contexts(contexts: Union[str, List[str]]) -> List[str]:
    """
    Normalize different input formats into a list of comma-separated strings.

    Examples:
    - 'main, master'           -> ['main', 'master']
    - ['main, master']         -> ['main', 'master']
    - ['main', 'master']       -> ['main', 'master']

    Args:
        contexts (Union[str, List[str]]): The input contexts to normalize.

    Returns:
        List[str]: A list of individual context strings.
    """
    if isinstance(contexts, str):
        return [c.strip() for c in contexts.split(',') if c.strip()]
    elif isinstance(contexts, list):
        result = []
        for c in contexts:
            result.extend([s.strip() for s in c.split(',') if s.strip()])
        return result
    return []
