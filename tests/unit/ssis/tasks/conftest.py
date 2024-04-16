from unittest.mock import patch

import pytest


@pytest.fixture()
def agent_state_mock():
    with patch("agents.ssis.tasks.AGENT_STATE") as mock:
        mock.last_known_execution_id = None
        yield mock
