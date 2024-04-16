import os
from unittest.mock import patch

import pytest


@pytest.fixture()
def databricks_config_data():
    config = {
        "jobs_version": "2.1",
        "host": "https://databricks.com",
        "endpoint": "https://databricks.com/",
        "method": "GET",
        "timeout": "1.0",
    }
    return config


@pytest.fixture()
def mock_databricks_env_vars(databricks_config_data):
    environment_variables = {"DK_DATABRICKS_" + k.upper(): str(v) for k, v in databricks_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables
