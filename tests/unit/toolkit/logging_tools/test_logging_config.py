import tempfile
from pathlib import Path
from uuid import uuid4

import pytest

from toolkit.logging_tools import logging_init


@pytest.mark.unit()
def test_init_logging_file_str():
    """Can initialize logging with a file logger with a string file path."""
    temp_dir = Path(tempfile.gettempdir())
    filename = f"{uuid4().hex}.log"
    file_path = temp_dir.joinpath(filename)
    assert file_path.exists() is False
    assert file_path.is_file() is False

    logging_init(logfile=str(file_path))
    assert file_path.is_file() is True


@pytest.mark.unit()
def test_init_logging_file_pathlib():
    """Can initialize logging with a file logger with a Path object file path."""
    temp_dir = Path(tempfile.gettempdir())
    filename = f"{uuid4().hex}.log"
    file_path = temp_dir.joinpath(filename)
    assert file_path.exists() is False
    assert file_path.is_file() is False

    logging_init(logfile=file_path)
    assert file_path.is_file() is True
