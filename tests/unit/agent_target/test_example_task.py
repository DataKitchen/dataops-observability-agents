import os
from unittest.mock import AsyncMock, patch

import pytest

from agents.target_example.configuration import ExampleConfiguration
from agents.target_example.task import WebsocketTask
from framework.core.channels import NullSendChannel
from registry import ConfigurationRegistry


@pytest.fixture()
def mock_core_env_vars(core_config_data):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in core_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def full_agent_config_env_vars(core_config_data, example_config_data):
    core = {"DK_" + k.upper(): str(v) for k, v in core_config_data.items()}
    example = {"DK_EXAMPLE_" + k.upper(): str(v) for k, v in example_config_data.items()}

    core.update(example)
    with patch.dict(os.environ, core):
        yield core


@pytest.mark.unit()
async def test_example_task(full_agent_config_env_vars):
    client = AsyncMock()
    ConfigurationRegistry().register("example", ExampleConfiguration)

    with patch("agents.target_example.task.WebsocketHandle") as handle:
        created_handle = AsyncMock()
        created_handle.pre_hook = AsyncMock()
        created_handle.handle = AsyncMock()
        created_handle.handle.return_value = {}
        handle.return_value = created_handle
        task = WebsocketTask(outbound_channel=NullSendChannel())

        await task.execute_task(client=client)
        handle.assert_called_once_with(client)
        created_handle.pre_hook.assert_called_once_with()
        created_handle.handle.assert_called_once_with()
        created_handle.post_hook.assert_called_once_with({})
