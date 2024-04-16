from http import HTTPMethod
from ssl import SSLContext
from time import time
from unittest.mock import Mock, patch

import pytest
from httpx import URL, AsyncClient, Auth, Headers, Response, Timeout
from httpx import __version__ as httpx_version
from pytest_httpserver import HTTPServer
from pytest_httpx import HTTPXMock
from trustme import CA

from framework import authenticators
from framework.authenticators import (
    SECONDS_BEFORE_EXP,
    AzureBasicOauthAuth,
    AzureServicePrincipalAuth,
    BasicAuth,
    TokenAuth,
    refresh_azure_basic_oauth_token,
    refresh_azure_service_principal_token,
)
from framework.configuration.http import HTTPClientConfig, ObservabilityHTTPClientConfig
from framework.core.handles import HTTPAPIRequestHandle, get_client

BASIC_AUTH = BasicAuth("tyler", "hunter2")
TOKEN_AUTH = TokenAuth("deadbeef42")


class TestHTTPAPIRequestHandle(HTTPAPIRequestHandle):
    method = HTTPMethod.GET
    path = "/foo"


class TestHTTPAPIRequestFormatHandle(HTTPAPIRequestHandle):
    method = HTTPMethod.GET
    path = "/bar/{foo}"


@pytest.fixture()
def payload():
    return {"Hello": "world"}


@pytest.fixture()
def base_httpx_headers():
    return Headers(
        {
            "host": "example.com",
            "accept": "*/*",
            "accept-encoding": "gzip, deflate",
            "connection": "keep-alive",
            "user-agent": f"python-httpx/{httpx_version}",
        },
    )


@pytest.fixture()
def setup_http_mock_request(httpx_mock: HTTPXMock, payload):
    payload = {"Hello": "world"}
    httpx_mock.add_response(status_code=200, json=payload)
    return httpx_mock


@pytest.fixture()
def mock_request_azure_spn_auth(httpx_mock: HTTPXMock):
    payload = {"access_token": "aC3s5ToK3n"}
    httpx_mock.add_response(status_code=200, json=payload)
    return httpx_mock


@pytest.fixture()
def expires_on():
    return time() + 3600


TEST_SERVER = "localhost"
TEST_PORT = "8000"


@pytest.fixture(scope="session")
def httpserver_ssl_context():
    ca = CA()
    context = SSLContext()
    localhost_cert = ca.issue_cert(TEST_SERVER)
    localhost_cert.configure_cert(context)
    return context


@pytest.fixture(scope="session")
def httpserver_listen_address():
    return TEST_SERVER, TEST_PORT


@pytest.mark.unit()
async def test_ssl_verify_true_failed(httpserver: HTTPServer, httpserver_listen_address, httpserver_ssl_context):
    httpserver.expect_request("/foo").respond_with_data("hello world!")
    async with get_client() as async_client:
        handle = TestHTTPAPIRequestHandle(base_url=URL(f"https://{TEST_SERVER}:{TEST_PORT}"), client=async_client)
        with pytest.raises(Exception, match=""):  # noqa: PT011
            await handle.handle()


@pytest.mark.unit()
@pytest.mark.parametrize("client_config", [HTTPClientConfig, ObservabilityHTTPClientConfig])
async def test_ssl_verify_false_ok(
    client_config,
    httpserver: HTTPServer,
    httpserver_listen_address,
    httpserver_ssl_context,
):
    config = client_config()
    config.ssl_verify = False
    httpserver.expect_request("/foo").respond_with_data("hello world!")
    async with get_client(config=config) as async_client:
        handle = TestHTTPAPIRequestHandle(base_url=URL(f"https://{TEST_SERVER}:{TEST_PORT}"), client=async_client)
        result = await handle.handle()
        assert result.text == "hello world!"


@pytest.mark.unit()
async def test_request_handle_post_hook(httpx_mock):
    payload = {"Hello": "world"}
    response = Response(status_code=200, json=payload)
    async with AsyncClient() as a:
        handle = HTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        result = await handle.post_hook(response=response)
        assert payload == result


@pytest.mark.unit()
async def test_request_handler_no_args(payload, base_httpx_headers, setup_http_mock_request: HTTPXMock):
    async with AsyncClient() as a:
        handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        response = await handle.handle()
        assert response.json() == payload
    request = setup_http_mock_request.get_request()
    assert request is not None
    assert request.url == URL("http://example.com" + TestHTTPAPIRequestHandle.path)
    assert request.method == TestHTTPAPIRequestHandle.method
    assert request.headers == base_httpx_headers


@pytest.mark.unit()
async def test_request_handler_path_args(payload, base_httpx_headers, setup_http_mock_request: HTTPXMock):
    async with AsyncClient() as a:
        path_args = {"foo": "bar"}
        handle = TestHTTPAPIRequestFormatHandle(base_url=URL("http://example.com"), client=a)
        response = await handle.handle(path_args=path_args)
        assert response.json() == payload
    request = setup_http_mock_request.get_request()
    assert request is not None
    assert request.url == URL("http://example.com" + (TestHTTPAPIRequestFormatHandle.path.format(**path_args)))
    assert request.method == TestHTTPAPIRequestFormatHandle.method
    assert request.headers == base_httpx_headers


@pytest.mark.unit()
async def test_request_handler_query(setup_http_mock_request: HTTPXMock):
    async with AsyncClient() as a:
        handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        await handle.handle(query_params={"arg": "10", "bar": "hello"})
    request = setup_http_mock_request.get_request()
    assert request is not None
    assert request.url == URL("http://example.com/foo?arg=10&bar=hello")


@pytest.mark.unit()
async def test_request_handler_headers(base_httpx_headers, setup_http_mock_request: HTTPXMock):
    headers = {"X-BAR": "fizz", "X-BAZ": "buzz"}
    async with AsyncClient() as a:
        handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        await handle.handle(headers=headers)
    request = setup_http_mock_request.get_request()
    base_httpx_headers.update(headers)
    assert request is not None
    assert request.headers == base_httpx_headers


@pytest.mark.unit()
async def test_token_auth(base_httpx_headers, setup_http_mock_request: HTTPXMock):
    headers = {"X-BAR": "fizz", "X-BAZ": "buzz"}
    async with AsyncClient(auth=TOKEN_AUTH) as a:
        handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        await handle.handle(headers=headers)
    request = setup_http_mock_request.get_request()
    expected_auth_header = "Bearer deadbeef42"
    assert request is not None
    assert expected_auth_header == request.headers.get("authorization")


@pytest.mark.unit()
def test_get_client():
    """Get client returns an AsyncClient instance with some defaults set."""
    async_client = get_client()
    assert isinstance(async_client, AsyncClient)
    assert isinstance(async_client.timeout, Timeout)
    assert async_client.auth is None


@pytest.mark.unit()
def test_get_client_overrides():
    """Get client can override configuration."""
    http_conf = HTTPClientConfig(auth=TOKEN_AUTH)
    async_client = get_client(http_conf)
    assert isinstance(async_client, AsyncClient)
    assert isinstance(async_client.timeout, Timeout)
    assert isinstance(async_client.auth, Auth)


@pytest.mark.unit()
async def test_refresh_azure_service_principal_token(mock_request_azure_spn_auth: HTTPXMock, mock_core_env_vars):
    token, expiration = await refresh_azure_service_principal_token("tenant-123", "client-1", "secret-1", "a_scope")
    expected_url = "https://login.microsoftonline.com/tenant-123/oauth2/v2.0/authorize"
    requests = mock_request_azure_spn_auth.get_requests()
    assert len(requests) == 1
    assert requests[0].method == "POST"
    assert requests[0].url == expected_url
    assert token == "aC3s5ToK3n"
    assert expiration > time()


@pytest.mark.unit()
async def test_azure_service_principal_auth(mock_request_azure_spn_auth: HTTPXMock, mock_core_env_vars):
    auth = AzureServicePrincipalAuth("tenant-123", "a_scope", "client-1", "secret-1")

    mock_request = Mock()
    mock_request.headers = {}
    assert auth.access_token is None
    assert auth.token_expiration <= time()
    assert auth._try_set_request_token(mock_request) is False
    assert mock_request.headers == {}

    async with AsyncClient(auth=auth) as a:
        handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
        await handle.handle()

    assert auth.access_token == "aC3s5ToK3n"
    assert auth.token_expiration > time()
    assert auth._try_set_request_token(mock_request) is True
    assert mock_request.headers == {"Authorization": "Bearer aC3s5ToK3n"}
    requests = mock_request_azure_spn_auth.get_requests()
    assert len(requests) == 2
    auth_request = requests[0]
    target_request = requests[1]
    assert auth_request.url == "https://login.microsoftonline.com/tenant-123/oauth2/v2.0/authorize"
    assert auth_request.method == "POST"
    assert auth_request.headers.get("authorization") is None
    assert target_request.headers.get("authorization") == "Bearer aC3s5ToK3n"
    assert target_request.method == "GET"


@pytest.mark.unit()
async def test_refresh_azure_basic_oauth_token(mock_core_env_vars, expires_on):
    mock_credential = Mock()
    mock_credential.get_token.return_value.token = "aC3s5ToK3n"
    mock_credential.get_token.return_value.expires_on = expires_on
    token, expiration = await refresh_azure_basic_oauth_token(mock_credential, "a-scope")

    mock_credential.get_token.assert_called_once_with("a-scope")
    assert token == "aC3s5ToK3n"
    assert expiration == expires_on - SECONDS_BEFORE_EXP


@pytest.mark.unit()
async def test_azure_basic_oauth_auth(mock_core_env_vars, expires_on):
    with patch.object(authenticators, "UsernamePasswordCredential"):
        auth = AzureBasicOauthAuth(
            "tenant-123",
            "client-1",
            "user-1",
            "secret-1",
            URL("https://azure.scope"),
            URL("https://login.url"),
        )

        mock_request = Mock()
        mock_request.headers = {}
        assert auth.access_token is None
        assert auth.token_expiration <= time()
        assert auth._try_set_request_token(mock_request) is False
        assert mock_request.headers == {}

        async with AsyncClient(auth=auth) as a:
            auth.credential.get_token.return_value.token = "aC3s5ToK3n"
            auth.credential.get_token.return_value.expires_on = expires_on
            handle = TestHTTPAPIRequestHandle(base_url=URL("http://example.com"), client=a)
            await handle.handle()

        assert auth.access_token == "aC3s5ToK3n"
        assert auth.token_expiration == expires_on - SECONDS_BEFORE_EXP
        assert auth._try_set_request_token(mock_request) is True
        assert mock_request.headers == {"Authorization": "Bearer aC3s5ToK3n"}
