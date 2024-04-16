from io import StringIO

import pytest

from framework.configuration import CoreConfiguration
from testlib.configurations.validators import check_all_core_config


@pytest.mark.unit()
def test_load_core_config(core_config_data):
    config = CoreConfiguration(**core_config_data)

    # test that API key is secret.
    with StringIO() as f:
        print(config.observability_service_account_key, file=f)
        contents = f.getvalue()
    assert all(c == "*" for c in contents.strip())

    check_all_core_config(core_config_data, config)


@pytest.mark.unit()
def test_auto_load_environment(mock_core_env_vars, core_config_data):
    config = CoreConfiguration()
    check_all_core_config(core_config_data, config)

    # now mix and match
    config = CoreConfiguration(observability_base_url=core_config_data["observability_base_url"])
    check_all_core_config(core_config_data, config)
