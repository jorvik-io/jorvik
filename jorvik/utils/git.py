'''This module provides utility functions to interact with git client commands.'''
import os
import subprocess
from jorvik.utils.paths import get_codefile_path

class GitUtilsError(Exception):
    """Custom exception for Git utility errors."""
    def __init__(self):
        message = "Not in a Git repository; git client not installed or unable to determine the current branch."
        super().__init__(message)

def get_current_git_branch() -> str:
    """Get the current Git branch name.

    Returns:
        str: The name of the current Git branch.

    Raises:
        GitUtilsError: If not in a Git repository or unable to determine the current branch.
    """

    original_cwd = os.getcwd()

    # Change to the directory containing the outermost code file
    code_file_path = get_codefile_path()
    if code_file_path.startswith("Unknown"):
        # If the code file path is unknown, assume the current working directory is the script directory
        script_dir = original_cwd
    else:
        script_dir = os.path.abspath(os.path.dirname(code_file_path))
    os.chdir(script_dir)

    try:
        # Run 'git branch --show-current' and capture the output
        result = subprocess.run(
            ['git', 'branch', '--show-current'],
            capture_output=True,
            text=True,
            check=True
        )
        current_branch = result.stdout.strip()
    except subprocess.CalledProcessError:
        raise GitUtilsError()
    finally:
        # Change back to the original directory
        os.chdir(original_cwd)

    return current_branch
