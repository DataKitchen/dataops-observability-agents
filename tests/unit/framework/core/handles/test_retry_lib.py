import json
import os
from datetime import UTC, datetime, timedelta
from http import HTTPMethod, HTTPStatus
from unittest.mock import patch

import pytest
from httpx import URL, Response

from framework.configuration import HTTPClientConfig
from framework.core.handles import HTTPAPIRequestHandle, get_client
from framework.core.handles.lib import has_retry_text, parse_rate_limit
from registry import ConfigurationRegistry


@pytest.fixture(scope="session")
def mock_core_env_vars():
    core_config_data = {
        "agent_type": "a",
        "observability_service_account_key": "b",
        "observability_base_url": "https://c.io/",
        "agent_key": "d",
    }
    environment_variables = {"DK_" + k.upper(): str(v) for k, v in core_config_data.items()}
    with patch.dict(os.environ, environment_variables):
        yield environment_variables


@pytest.fixture(scope="session")
def http_config(mock_core_env_vars):
    return ConfigurationRegistry().lookup("http", HTTPClientConfig)


@pytest.fixture(scope="session")
def rate_limit_header_response():
    """An httpx response with a rate limit header."""
    _headers = {"Content-Type": "application/json", "X-RateLimit-Reset": "0.1"}
    content = json.dumps({}, indent=4).encode("utf-8")
    return Response(status_code=HTTPStatus.BAD_REQUEST, content=content, headers=_headers)


@pytest.fixture(scope="session")
def rate_limit_content_response():
    """An httpx response with a textual rate limit header."""
    _headers = {"Content-Type": "text/html"}
    content = b"<html><body><p>You are rate limited; please try again in a bit.</p></body></html>"
    return Response(status_code=HTTPStatus.UNAUTHORIZED, content=content, headers=_headers)


@pytest.fixture(scope="session")
def response_200():
    """An httpx response without a rate limit header."""
    _headers = {"Content-Type": "application/json"}
    content = json.dumps({}, indent=4).encode("utf-8")
    return Response(status_code=HTTPStatus.OK, content=content, headers=_headers)


@pytest.mark.unit()
def test_parse_rate_limit_timestamps(http_config):
    """Rate limit header values which represent a timestamp are parsed into a wait time."""
    now = datetime.now(UTC) + timedelta(minutes=2)
    result = parse_rate_limit(now.timestamp())
    assert 121.0 >= result


@pytest.mark.unit()
def test_parse_rate_limit_wait():
    """Rate limit header values which represent a wait time are returned unmodified."""
    result = parse_rate_limit(5.0)
    assert 5.0 == result


@pytest.mark.unit()
def test_parse_rate_limit_never_exceeds_read_timeout(http_config):
    """A parsed rate limit value never exceeds the default read timeout value."""
    read_timeout = http_config.read_timeout
    parsed = parse_rate_limit(read_timeout + 1.0)
    assert read_timeout >= parsed


@pytest.mark.unit()
def test_has_retry_text_true():
    """The `has_retry_text` function detects retry indicators in response text."""
    value = "Rate limit reached, please try again in a bit."
    assert has_retry_text(value) is True


@pytest.mark.unit()
@pytest.mark.parametrize("response_value", argvalues=("200", 200, None, object(), "<http></http>", "{}"))
def test_has_retry_text_false(response_value):
    """The `has_retry_text` function yields False when presented with invalid response data."""
    result = has_retry_text(response_value)
    assert result is False


class FakeEndpoint(HTTPAPIRequestHandle):
    path = "fake"
    method = HTTPMethod.GET


@pytest.mark.unit()
async def test_retry_headers(rate_limit_header_response, response_200):
    """Retry headers cause additional requests to be dispatched."""
    endpoint = FakeEndpoint(base_url=URL("http://fake.test/"), client=get_client())

    with patch.object(endpoint.client, "request") as m:
        m.side_effect = [rate_limit_header_response, rate_limit_header_response, response_200]
        r = await endpoint.handle()
        total_calls = m.call_count

        assert HTTPStatus.OK == r.status_code
        assert 3 == total_calls


@pytest.mark.unit()
async def test_retry_content(rate_limit_content_response, response_200):
    """Retry indicators in response content cause additional requests to be dispatched."""
    endpoint = FakeEndpoint(base_url=URL("http://fake.test/"), client=get_client())

    with patch.object(endpoint.client, "request") as m:
        m.side_effect = [rate_limit_content_response, response_200]
        r = await endpoint.handle()
        total_calls = m.call_count

        assert HTTPStatus.OK == r.status_code
        assert 2 == total_calls
