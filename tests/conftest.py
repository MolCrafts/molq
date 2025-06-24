import os
import shutil
import tempfile
from pathlib import Path

import pytest


@pytest.fixture(scope="session", autouse=True)
def temp_workdir():
    """Change working directory to a temporary location for all tests."""
    temp_dir = tempfile.mkdtemp(prefix="molexp_test_")
    original_cwd = os.getcwd()
    os.chdir(temp_dir)
    yield Path(temp_dir)
    os.chdir(original_cwd)
    shutil.rmtree(temp_dir)


@pytest.fixture
def tmp_molq_home(tmp_path, monkeypatch):
    """Create a temporary HOME directory for molq database isolation."""
    home_dir = tmp_path / "molq_home"
    home_dir.mkdir()

    # Create .molq directory for molq configuration and database
    molq_dir = home_dir / ".molq"
    molq_dir.mkdir()

    # Set HOME environment variable to the temporary directory
    monkeypatch.setenv("HOME", str(home_dir))

    return home_dir


@pytest.fixture
def isolated_temp_dir(tmp_path):
    """Create an isolated temporary directory for individual test operations."""
    test_dir = tmp_path / "test_operations"
    test_dir.mkdir()
    return test_dir


@pytest.fixture
def cleanup_after_test():
    """Ensure cleanup happens after each test."""
    # Setup phase
    created_files = []
    created_dirs = []

    def register_file(filepath):
        created_files.append(filepath)

    def register_dir(dirpath):
        created_dirs.append(dirpath)

    # Yield cleanup functions to the test
    yield {"register_file": register_file, "register_dir": register_dir}

    # Cleanup phase
    for filepath in created_files:
        if os.path.exists(filepath):
            try:
                os.remove(filepath)
            except OSError:
                pass

    for dirpath in created_dirs:
        if os.path.exists(dirpath):
            try:
                shutil.rmtree(dirpath)
            except OSError:
                pass


@pytest.fixture
def mock_job_environment(tmp_path, monkeypatch):
    """Create a complete mock environment for job operations."""
    # Create directory structure
    job_env = tmp_path / "job_env"
    job_env.mkdir()

    home_dir = job_env / "home"
    home_dir.mkdir()

    molq_dir = home_dir / ".molq"
    molq_dir.mkdir()

    workdir = job_env / "workdir"
    workdir.mkdir()

    logdir = job_env / "logs"
    logdir.mkdir()

    # Save current directory
    original_cwd = os.getcwd()

    # Set up environment variables
    monkeypatch.setenv("HOME", str(home_dir))
    monkeypatch.setenv("MOLQ_WORKDIR", str(workdir))
    monkeypatch.setenv("MOLQ_LOGDIR", str(logdir))

    # Change to work directory
    os.chdir(workdir)

    yield {
        "root": job_env,
        "home": home_dir,
        "molq_dir": molq_dir,
        "workdir": workdir,
        "logdir": logdir,
    }

    # Restore original directory
    os.chdir(original_cwd)


@pytest.fixture
def test_script_dir(tmp_path):
    """Create a directory for test scripts with automatic cleanup."""
    script_dir = tmp_path / "scripts"
    script_dir.mkdir()

    # Create a simple test script
    test_script = script_dir / "test_script.py"
    test_script.write_text(
        """#!/usr/bin/env python3
import sys
import time
import os

print(f"Test script running with args: {sys.argv[1:]}")
print(f"Working directory: {os.getcwd()}")
print("Test script completed successfully")
"""
    )
    test_script.chmod(0o755)

    return script_dir
