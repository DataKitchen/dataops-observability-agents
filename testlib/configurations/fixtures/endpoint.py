import os
from unittest.mock import patch

import pytest


@pytest.fixture()
def endpoint_config_data():
    config = {
        "endpoint": "https://databricks.com/",
        "method": "GET",
        "timeout": 1.0,
    }
    return config


@pytest.fixture()
def mock_endpoint_env_vars(endpoint_config_data):
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in endpoint_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables
