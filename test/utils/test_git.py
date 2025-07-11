from unittest import mock
from jorvik.utils.git import get_current_git_branch, GitUtilsError
import subprocess
import pytest

@mock.patch("jorvik.utils.git.subprocess.run")
def test_get_current_git_branch_success(mock_subprocess_run):
    # Mock subprocess.run to return a predefined branch name "dev"
    mock_subprocess_run.return_value = mock.Mock(stdout="dev")
    branch = get_current_git_branch()
    assert branch == "dev"

@mock.patch("jorvik.utils.git.subprocess.run")
def test_get_current_git_branch_subprocess_error(mock_subprocess_run):
    # Mock subprocess.run to raise a CalledProcessError
    mock_subprocess_run.side_effect = subprocess.CalledProcessError(returncode=1, cmd='git branch --show-current')
    with pytest.raises(GitUtilsError):
        get_current_git_branch()
