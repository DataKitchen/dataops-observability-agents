import pytest

from agents.airflow.configuration import AirflowConfiguration


@pytest.mark.unit()
def test_airflow_configuration(airflow_config_data):
    config = AirflowConfiguration(**airflow_config_data)
    assert str(config.api_url) == airflow_config_data["api_url"] + "/"
