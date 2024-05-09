from framework.configuration import CoreConfiguration


def check_all_core_config(data: dict, actual: CoreConfiguration) -> None:
    assert data["agent_type"] == actual.agent_type
    assert data["observability_service_account_key"] == actual.observability_service_account_key.get_secret_value()
    assert data["observability_base_url"] + "/" == str(actual.observability_base_url)
    assert data["log_level"] == str(actual.log_level)
