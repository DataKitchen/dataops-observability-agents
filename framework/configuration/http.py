from pathlib import Path

from httpx import Auth
from pydantic import NonNegativeFloat, NonNegativeInt
from pydantic_settings import BaseSettings, SettingsConfigDict


class HTTPClientConfig(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="DK_HTTP_", extra="allow")
    """Ensures all environment variables are read from DK_HTTP_<key-name>."""

    read_timeout: NonNegativeFloat = 60.0
    """Maximum time (in seconds) to wait for data to be received."""

    write_timeout: NonNegativeFloat = 30.0
    """Maximum time (in seconds) to wait for data to be sent."""

    connection_timeout: NonNegativeFloat = 10.0
    """Maximum time (in seconds) to wait for a socket connection to a host."""

    pool_timeout: NonNegativeFloat = 10.0
    """Maximum time (in seconds) to wait for a connection from the connection pool."""

    retries: NonNegativeInt = 3
    """Number of times to retry a failed connection."""

    max_total_connections: NonNegativeInt = 10
    """Maximum connections to each host in a connection pool."""

    max_keepalive_connections: NonNegativeInt = 5
    """Maximum number of keepalive connections."""

    keepalive_expiration: NonNegativeInt = 10
    """Timelimit on idle keep-alive connections (in seconds)."""

    follow_redirects: bool = True
    """Whether or not to follow redirects."""

    auth: None | tuple[str | bytes, str | bytes] | Auth = None
    """Either a username/password tuple or an instance of httpx.Auth."""

    http2: bool = False
    """Enable/disable HTTP/2 support. Defaults to False."""

    params: dict[str, str] | list[tuple[str, str]] | tuple[tuple[str, str], ...] | None = None
    """Query parameters to include with every request."""

    ssl_verify: bool = True
    """Enable/disable SSL verification."""

    ssl_cert_file: None | str | Path = None
    """
    When the SSL verification is enabled, use the certificate file at the provided path.
    If None, the default certificate bundle is used.
    """


class ObservabilityHTTPClientConfig(HTTPClientConfig):
    model_config = SettingsConfigDict(env_prefix="DK_OBSERVABILITY_", extra="allow")
