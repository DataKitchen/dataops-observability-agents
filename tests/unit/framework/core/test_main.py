import os
from unittest.mock import patch
from uuid import uuid4

import pytest

from framework.__main__ import main

pytest.fixture(autouse=True)


def dont_read_agent_toml():
    filename = [str(uuid4)]
    with patch("framework.configuration.core.DEFAULT_CONFIGURATION_FILE_PATHS", filename) as f:
        yield f


@pytest.fixture()
def patch_databricks_agent():
    with patch("framework.__main__.databricks_agent") as p:
        yield p


@pytest.fixture()
def patch_eventhub_agent():
    with patch("framework.__main__.eventhub_agent") as p:
        yield p


@pytest.fixture()
def patch_example_agent():
    with patch("framework.__main__.example_agent") as p:
        yield p


@pytest.fixture()
def patch_configure_logging():
    with patch("framework.__main__.logging_init") as p:
        yield p


async def run_main(mocked_main, patch_configure_logging, configuration_data, agent_type):
    configuration_data["agent_type"] = agent_type
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in configuration_data.items()}
    with patch.dict(os.environ, environment_variables):
        await main()
    mocked_main.assert_awaited_once()
    patch_configure_logging.assert_called_once()


@pytest.mark.unit()
async def test_main_databricks_agent(patch_databricks_agent, patch_configure_logging, core_config_data):
    await run_main(patch_databricks_agent, patch_configure_logging, core_config_data, "databricks")


@pytest.mark.unit()
async def test_main_example_agent(patch_example_agent, patch_configure_logging, core_config_data):
    await run_main(patch_example_agent, patch_configure_logging, core_config_data, "example_agent")


@pytest.mark.unit()
async def test_main_eventhubs_agent(patch_eventhub_agent, patch_configure_logging, core_config_data):
    await run_main(patch_eventhub_agent, patch_configure_logging, core_config_data, "eventhubs")


@pytest.mark.unit()
async def test_main_wrong_agent(mock_core_env_vars):
    with pytest.raises(SystemExit):
        with patch.dict(os.environ, {"DK_AGENT_TYPE": "nonexistent_agent"}):
            await main()
