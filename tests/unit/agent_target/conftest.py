import os
from unittest.mock import patch

import pytest

from agents.target_example.configuration import ExampleConfiguration


@pytest.fixture()
def example_config_data():
    data = {"target_url": "ws://127.0.0.1/ws", "component_type": "batch_pipeline", "timeout": 5000.0, "period": 1.0}

    return data


@pytest.fixture()
def mock_example_env_vars(example_config_data):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in example_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture()
def example_config(example_config_data):
    return ExampleConfiguration(**example_config_data)
