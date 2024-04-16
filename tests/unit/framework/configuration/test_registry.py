import os
import tempfile
from copy import deepcopy
from unittest.mock import patch

import pytest
import tomli_w

from agents.databricks.configuration import DatabricksConfiguration
from framework.configuration import CoreConfiguration, HTTPClientConfig
from framework.configuration.http import ObservabilityHTTPClientConfig
from registry.configuration_registry import ConfigurationRegistry


@pytest.fixture()
def temporary_toml(core_config_data, http_config_data):
    with tempfile.NamedTemporaryFile(mode="wb") as temp:
        # Write some example TOML data to the temporary file
        second_http = deepcopy(http_config_data)
        second_http["verify"] = "cert.ssl"
        tomli_w.dump({"core": core_config_data, "http": http_config_data, "http2": second_http}, temp)
        temp.flush()
        temp.seek(0)
        yield temp


@pytest.fixture()
def mocked_configuration_file(temporary_toml):
    with patch("registry.configuration_registry.DEFAULT_CONFIGURATION_FILE_PATHS", [temporary_toml.name]) as f:
        yield f


@pytest.mark.unit()
def test_default_loads_with_file(mocked_configuration_file):
    assert not ConfigurationRegistry.__initialized_configurations__
    registry = ConfigurationRegistry()
    assert len(ConfigurationRegistry.__initialized_configurations__) == 3

    assert "core" in ConfigurationRegistry.__initialized_configurations__
    assert isinstance(registry.lookup("core", CoreConfiguration), CoreConfiguration)
    assert "http" in ConfigurationRegistry.__initialized_configurations__
    assert isinstance(registry.lookup("http", HTTPClientConfig), HTTPClientConfig)
    assert "observability" in ConfigurationRegistry.__initialized_configurations__
    assert isinstance(registry.lookup("observability", ObservabilityHTTPClientConfig), ObservabilityHTTPClientConfig)


@pytest.mark.unit()
def test_shared_state_registry(mocked_configuration_file):
    first_registry = ConfigurationRegistry()
    second_registry = ConfigurationRegistry()

    second_registry.register("http2", HTTPClientConfig)
    config = first_registry.lookup("http2", HTTPClientConfig)
    assert config.verify == "cert.ssl"


@pytest.mark.unit()
def test_register_duplicate(mocked_configuration_file):
    registry = ConfigurationRegistry()
    with pytest.raises(KeyError):
        registry.register("core", CoreConfiguration)


@pytest.mark.unit()
def test_registry_load_from_environment(mocked_configuration_file, mock_databricks_env_vars):
    registry = ConfigurationRegistry()
    registry.register("databricks", DatabricksConfiguration)
    config = registry.lookup("databricks", DatabricksConfiguration)
    assert isinstance(config, DatabricksConfiguration)


@pytest.mark.unit()
def test_registry_unknown_lookup(mock_core_env_vars):
    registry = ConfigurationRegistry()
    config_name = "randomconfig"
    with pytest.raises(KeyError) as e:
        registry.lookup(config_name, CoreConfiguration)
    assert e.value.args[0] == f"Unknown configuration {config_name}, register() configuration."


@pytest.mark.unit()
def test_registry_add(core_config, mock_core_env_vars):
    core_config.log_level = "error"
    registry = ConfigurationRegistry()
    registry.add("core", core_config)
    config = registry.lookup("core", CoreConfiguration)

    assert config.log_level == "error"


@pytest.mark.unit()
def test_registry_mutate(mock_core_env_vars):
    registry = ConfigurationRegistry()
    core = registry.mutate("core", CoreConfiguration, log_level="error")
    assert core.log_level == "error"


@pytest.mark.unit()
def test_registry_available(mock_core_env_vars, databricks_config_data):
    registry = ConfigurationRegistry()
    is_core_available = registry.available("core", CoreConfiguration)
    assert is_core_available is True
    # databricks configuration is not available
    is_databricks_available = registry.available("databricks", DatabricksConfiguration)
    assert is_databricks_available is False

    # make the configuration available
    environment_variables = {"DK_DATABRICKS_" + k.upper(): str(v) for k, v in databricks_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        is_databricks_available = registry.available("databricks", DatabricksConfiguration)
        assert is_databricks_available is True

    # configuration should be registered from calling available above
    is_databricks_available = registry.available("databricks", DatabricksConfiguration)
    assert is_databricks_available is True
