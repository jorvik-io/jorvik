from pyspark.sql import SparkSession

from jorvik.storage.basic import BasicStorage
from jorvik.data_lineage.observer import DataLineageLogger
from jorvik.storage.protocols import Storage


def configure(track_lineage: bool = True) -> Storage:
    """ Configure the storage.
        Args:
            track_lineage (bool): Whether to track data lineage. Default is True.
        Returns:
            Storage: The configured storage instance.
    """
    st = BasicStorage()
    conf = SparkSession.getActiveSession().sparkContext.getConf()
    lineage_log_path = conf.get('io.jorvik.data_lineage.log_path', '')
    if track_lineage and lineage_log_path:
        st.register_output_observer(DataLineageLogger(lineage_log_path))
    return st


__all__ = ["Storage", "configure"]
