import inspect
import os
from jorvik.utils.databricks import get_notebook_path, DatabricksUtilsError

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
        # Filter out system or interactive files
        if file and not file.startswith('<') and os.path.exists(file):
            return file

    return "Unknown code file path"
