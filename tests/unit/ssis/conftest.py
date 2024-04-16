import datetime
from unittest.mock import patch

import pytest

from agents.ssis.agent_state import ExecutionState


@pytest.fixture()
def execution_start_time():
    return datetime.datetime(2023, 11, 30, 22, 12, 00, tzinfo=datetime.UTC)


@pytest.fixture()
def execution_end_time(execution_start_time):
    return execution_start_time + datetime.timedelta(minutes=5)


@pytest.fixture()
def package_data():
    return {
        "folder_name": "folder_1",
        "project_name": "project_1",
        "package_name": "package_1",
    }


@pytest.fixture()
def async_conn_mock():
    with patch("agents.ssis.database.AsyncConn") as mock:
        yield mock


@pytest.fixture()
def execution_state():
    return ExecutionState(execution_id=1)
