import inspect
from jorvik.utils.databricks import get_notebook_path, DatabricksUtilsError
import re

def is_site_package_path(path: str) -> bool:
    """Check if the given path is a site-packages directory
    Example:
        is_site_package_path("/usr/local/lib/python3.11/site-packages/numpy") -> True
        is_site_package_path("/some/other/path") -> False
    Args:
        path (str): The path to check
    Returns:
        bool: True if the path is a site-packages directory, False otherwise
    """
    pattern = r'/python[^/]+/site-packages/'
    return re.search(pattern, path) is not None

def is_notebook():
    """Check if the current environment is a Jupyter notebook or similar interactive environment.
    Returns:
        bool: True if running in a notebook, False otherwise.
    """
    try:
        from IPython import get_ipython
        shell = get_ipython().__class__.__name__
        return shell != 'NoneType'
    except (NameError, ImportError):
        return False  # Probably a .py file or other non-notebook environment

def get_codefile_path() -> str:
    """Get the path of the first (bottom-most) Python code file in the call stack
    Returns:
        str: Path of the code file
    """
    # If running in an interactive notebook, check if the running environment is Databricks and return the notebook path
    if is_notebook():
        try:
            return get_notebook_path()
        except DatabricksUtilsError:
            return "Unknown notebook path"

    # __file__ cannot be used because it refers to the file where that function is defined, not where it's called from
    stack = inspect.stack()
    for frame_info in reversed(stack):
        file = frame_info.filename
        if file and (not is_site_package_path(file)) and ('pytest' not in file):
            return file

    return "Unknown code file path"
