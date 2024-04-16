import pytest


@pytest.fixture()
def http_config_data():
    return {
        "read_timeout": 10.0,
        "write_timeout": 40.0,
        "connection_timeout": 1.0,
        "pool_timeout": 1.0,
        "retries": 4,
        "max_total_connections": 13,
        "max_keepalive_connections": 6,
        "keepalive_expiration": 11,
        "follow_redirects": True,
        "http2": False,
        "ssl_verify": False,
    }
