from unittest import mock
from jorvik.utils.paths import is_notebook, is_site_package_path, get_codefile_path

def test_is_notebook():
    assert is_notebook() is False, "is_notebook should return False in a standard Python environment"

def test_is_site_package_path():
    assert is_site_package_path("/usr/local/lib/python3.11/site-packages/numpy") is True
    assert is_site_package_path("/some/other/path") is False

def test_get_code_file_path_non_notebook():
    code_file_path = get_codefile_path()
    assert code_file_path.endswith("test/utils/test_paths.py")

@mock.patch("jorvik.utils.paths.is_notebook")
def test_get_code_file_path_unknown_notebook(mock_is_notebook):
    mock_is_notebook.return_value = True
    assert get_codefile_path() == "Unknown notebook path"

@mock.patch("jorvik.utils.paths.is_notebook")
@mock.patch('jorvik.utils.paths.get_notebook_path')
def test_get_code_file_path_databricks_notebook(mock_get_notebook_path, mock_is_notebook):
    mock_get_notebook_path.return_value = "Workspace/Path/To/Notebook"
    mock_is_notebook.return_value = True
    assert get_codefile_path() == "Workspace/Path/To/Notebook"
