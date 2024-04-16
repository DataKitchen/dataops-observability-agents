import logging
from pathlib import Path
from typing import Any, cast

from httpx import AsyncClient, AsyncHTTPTransport, Limits, Timeout

from framework.configuration.http import HTTPClientConfig

LOG = logging.getLogger(__name__)


DEFAULT_HTTP_SETTINGS = HTTPClientConfig()
"""Instance with default settings."""


def _get_verify_value(config: HTTPClientConfig) -> str | bool:
    match (config.ssl_verify, config.ssl_cert_file):
        case False, _:
            return False
        case True, str():
            return cast(str, config.ssl_cert_file)
        case True, Path():
            return str(cast(Path, config.ssl_cert_file).resolve())
        case _:
            return True


def get_client(config: HTTPClientConfig = DEFAULT_HTTP_SETTINGS) -> AsyncClient:
    """Create and return an AsyncClient instance."""

    # NOTE: Our hint for `params` is a subset of what httpx actually takes (theirs is huge); but since that causes the
    # analysis to complain, we cast it as Any when passing the argument. The linter will still validate our narrow hint
    return AsyncClient(
        auth=config.auth,
        params=cast(Any, config.params),
        follow_redirects=config.follow_redirects,
        timeout=Timeout(
            connect=config.connection_timeout,
            read=config.read_timeout,
            write=config.write_timeout,
            pool=config.pool_timeout,
        ),
        transport=AsyncHTTPTransport(verify=_get_verify_value(config), retries=config.retries),
        limits=Limits(
            max_keepalive_connections=config.max_keepalive_connections,
            max_connections=config.max_total_connections,
            keepalive_expiry=config.keepalive_expiration,
        ),
    )
