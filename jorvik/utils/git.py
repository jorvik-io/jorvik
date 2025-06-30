'''This module provides utility functions to interact with git client commands.'''
import os
import subprocess

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
        RuntimeError: If not in a Git repository or unable to determine the current branch.
    """

    original_cwd = os.getcwd()

    try:
        # Change to the directory containing this code file
        script_dir = os.path.abspath(os.path.dirname(__file__))
        os.chdir(script_dir)
    except NameError:
        # Code runs in an interactive environment where __file__ is not defined
        # Assume the current working directory is the script directory
        script_dir = original_cwd

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
