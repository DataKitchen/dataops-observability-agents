import pytest

from framework.configuration.http import HTTPClientConfig
from framework.core.handles.client import _get_verify_value


@pytest.mark.unit()
@pytest.mark.parametrize(
    ("expected_value", "input_values"),
    [
        (False, {"ssl_verify": False}),
        (False, {"ssl_verify": False, "ssl_cert_file": "/some/file"}),
        (True, {"ssl_verify": None}),
        (True, {"ssl_verify": True}),
        ("/some/file", {"ssl_verify": True, "ssl_cert_file": "/some/file"}),
        ("/some/file", {"ssl_verify": None, "ssl_cert_file": "/some/file"}),
    ],
)
def test_get_ssl_value(expected_value, input_values, http_config_data):
    config_data = {}
    config_data.update(http_config_data)
    config_data.update(input_values)

    for key, value in input_values.items():
        if value is None:
            del config_data[key]

    config = HTTPClientConfig(**config_data)

    assert _get_verify_value(config) == expected_value
