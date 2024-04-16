import os
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from agents.target_example.configuration import ExampleConfiguration
from testlib.configurations.helpers import better_validation_message


def check_example_config(data: dict, actual: ExampleConfiguration):
    assert data["target_url"] == str(actual.target_url)
    assert data["component_type"].upper() == actual.component_type
    assert data["period"] == actual.period
    assert data["timeout"] == actual.timeout


@pytest.mark.unit()
def test_load_config(example_config_data):
    with better_validation_message():
        config = ExampleConfiguration(**example_config_data)
    check_example_config(example_config_data, config)


@pytest.fixture()
def mock_example_env_vars(example_config_data):
    environment_variables = {"DK_EXAMPLE_" + k.upper(): str(v) for k, v in example_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.mark.unit()
def test_load_example_config_environment(mock_example_env_vars, mock_core_env_vars, example_config_data):
    with better_validation_message():
        config = ExampleConfiguration()
    check_example_config(example_config_data, config)


@pytest.mark.unit()
@pytest.mark.parametrize("parameter", argvalues=["target_url"])
def test_load_endpoint_config_missing_data(example_config_data, parameter):
    # only loading common data, so we're missing all the rest-specific items
    example_config_data.pop(parameter)
    with pytest.raises(ValidationError) as f:
        ExampleConfiguration(**example_config_data)
    assert parameter in f.value.errors()[0]["loc"]


@pytest.mark.unit()
def test_load_example_config_dataset(example_config_data):
    example_config_data["component_type"] = "dataset"
    with better_validation_message():
        config = ExampleConfiguration(**example_config_data)
    check_example_config(example_config_data, config)
