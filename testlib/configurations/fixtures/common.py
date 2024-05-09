import os
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from framework.configuration import CoreConfiguration
from registry import ConfigurationRegistry


# The config registry is designed to persist the config state across registry instances. Therefore, it has to be
# explicitly cleared state between test to not cause order dependencies.
@pytest.fixture(autouse=True)
def _clear_config_registry() -> None:
    ConfigurationRegistry.__initialized_configurations__.clear()


@pytest.fixture()
def mock_core_env_vars(core_config_data):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in core_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def core_config_data():
    return {
        "agent_type": "databricks",
        # No assigned meaning.
        "observability_service_account_key": "2wfLQIxdpeEBBULxnVqxODaKK2-o97WuqgOZxN2ex3d4JEwnMayp_oK55xZE8w31sgdL8f2Smpn3lUGv_Msiep7gm5BB7FzO",
        "observability_base_url": "https://test.domain.com/api",
        "log_level": "warning",
        "agent_key": "agent test key",
    }


@pytest.fixture()
def core_config(core_config_data):
    return CoreConfiguration(**core_config_data)


@pytest.fixture()
def now():
    return datetime.now(tz=UTC)
