import pytest

from agents.powerbi.config import PowerBIConfiguration


@pytest.mark.unit()
def test_base_api_url_has_default_value(env_powerbi_config):
    config = PowerBIConfiguration()
    assert str(config.base_api_url) in [
        "https://api.powerbi.com/v1.0/myorg",
        "https://api.powerbi.com/v1.0/myorg/",
    ]


@pytest.mark.unit()
def test_period_has_default_value(env_powerbi_config):
    config = PowerBIConfiguration()
    assert config.period == 5


@pytest.mark.unit()
def test_datasets_fetching_period_has_default_value(mock_core_env_vars, env_powerbi_config):
    config = PowerBIConfiguration()
    assert config.datasets_fetching_period == 15.0
