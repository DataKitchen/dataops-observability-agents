import pytest
from pydantic import ValidationError

from framework.configuration import EndpointConfiguration
from testlib.configurations.helpers import better_validation_message


def check_endpoint_config(data: dict, actual: EndpointConfiguration) -> None:
    assert data["endpoint"] == str(actual.endpoint)
    assert data["method"].upper() == actual.method
    assert data["timeout"] == actual.timeout


@pytest.mark.unit()
def test_load_endpoint_config(endpoint_config_data):
    with better_validation_message():
        config = EndpointConfiguration(**endpoint_config_data)
    check_endpoint_config(endpoint_config_data, config)


@pytest.mark.unit()
def test_load_endpoint_config_environment(endpoint_config_data, mock_endpoint_env_vars):
    with better_validation_message():
        config = EndpointConfiguration()
    check_endpoint_config(endpoint_config_data, config)


@pytest.mark.unit()
def test_load_endpoint_config_missing_data(endpoint_config_data):
    # only loading common data, so we're missing all the rest-specific items
    endpoint_config_data.pop("endpoint")
    with pytest.raises(ValidationError) as f:
        EndpointConfiguration(**endpoint_config_data)
    assert "endpoint" in f.value.errors()[0]["loc"]
