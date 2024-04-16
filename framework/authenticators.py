__all__ = (
    "TokenAuth",
    "BasicAuth",
    "AzureServicePrincipalAuth",
    "refresh_azure_service_principal_token",
    "AzureBasicOauthAuth",
    "refresh_azure_basic_oauth_token",
    "SECONDS_BEFORE_EXP",
)

import logging
from collections.abc import AsyncGenerator, Generator
from time import time
from typing import cast

from azure.identity import UsernamePasswordCredential
from httpx import Auth, BasicAuth, Request, Response
from pydantic import HttpUrl
from trio import Lock

from framework.configuration.http import HTTPClientConfig
from framework.core.handles.client import get_client
from registry.configuration_registry import ConfigurationRegistry

LOGGER = logging.getLogger(__name__)

SECONDS_BEFORE_EXP = 300
"""Buffer time in seconds before the token expiration"""


class TokenAuth(Auth):
    def __init__(self, token: str, *, token_prefix: str = "Bearer", header_name: str = "Authorization") -> None:
        self.base_token = token
        self.token = f"{token_prefix} {token}".strip()
        self.header_name = header_name

    def auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        # Send the request, with a custom header.
        request.headers[self.header_name] = self.token
        yield request


class AzureAuth(Auth):
    def __init__(self) -> None:
        self.access_token: str | None = None
        self.token_expiration = time()
        self.async_auth_lock = Lock()

    def _try_set_request_token(self, request: Request) -> bool:
        if self.access_token and time() < self.token_expiration:
            request.headers["Authorization"] = f"Bearer {self.access_token}"
            return True
        return False

    async def _async_refresh_token(self, request: Request) -> None:
        self.access_token, self.token_expiration = await self._call_auth_endpoint()
        request.headers["Authorization"] = f"Bearer {self.access_token}"

    async def _call_auth_endpoint(self) -> tuple[str, float]:
        raise NotImplementedError

    async def async_auth_flow(self, request: Request) -> AsyncGenerator[Request, Response]:
        if not self._try_set_request_token(request):
            async with self.async_auth_lock:
                # another request instance may have refreshed the token while lock was waiting
                if not self._try_set_request_token(request):
                    await self._async_refresh_token(request)
        yield request

    def sync_auth_flow(self, request: Request) -> Generator[Request, Response, None]:
        raise RuntimeError(f"Cannot use {self.__class__} with httpx.Client, use httpx.AsyncClient instead.")


async def refresh_azure_service_principal_token(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    scope: str,
) -> tuple[str, float]:
    url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/authorize"
    payload = {
        "grant_type": "client_credentials",
        "scope": scope,
        "client_id": client_id,
        "client_secret": client_secret,
    }
    LOGGER.info("Refreshing Azure Service Principal token")
    http_config = ConfigurationRegistry().mutate("http", HTTPClientConfig, auth=None)
    httpx_client = get_client(http_config)
    async with httpx_client:
        response = await httpx_client.post(url, data=payload)
    access_token = cast(str, response.json()["access_token"])
    # Azure Service Principal tokens are valid for an hour, no need to decode token for EXP value
    token_expiration = time() + (3600 - SECONDS_BEFORE_EXP)
    LOGGER.info("Azure Service Principal token refreshed")
    return access_token, token_expiration


async def refresh_azure_basic_oauth_token(credential: UsernamePasswordCredential, scope: str) -> tuple[str, float]:
    LOGGER.info("Refreshing Azure OAuth basic token")
    token = credential.get_token(scope)
    LOGGER.info("Azure OAuth basic token refreshed")
    return token.token, float(token.expires_on - SECONDS_BEFORE_EXP)


class AzureServicePrincipalAuth(AzureAuth):
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, scope: str) -> None:
        super().__init__()
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope

    async def _call_auth_endpoint(self) -> tuple[str, float]:
        return await refresh_azure_service_principal_token(
            self.tenant_id,
            self.client_id,
            self.client_secret,
            self.scope,
        )


class AzureBasicOauthAuth(AzureAuth):
    def __init__(  # noqa: PLR0913
        self,
        tenant_id: str,
        client_id: str,
        username: str,
        password: str,
        scope: str,
        authority: HttpUrl,
    ) -> None:
        super().__init__()
        self.credential = UsernamePasswordCredential(
            authority=str(authority),
            client_id=client_id,
            username=username,
            password=password,
            tenant_id=tenant_id,
        )
        self.scope = scope

    async def _call_auth_endpoint(self) -> tuple[str, float]:
        return await refresh_azure_basic_oauth_token(self.credential, self.scope)
