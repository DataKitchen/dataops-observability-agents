import logging
from dataclasses import dataclass
from http import HTTPMethod, HTTPStatus

import trio
from httpx import URL, AsyncClient, Response, TransportError

from framework.core.handles import Handle
from toolkit.more_typing import JSON_DICT

from .lib import has_retry_text, parse_rate_limit

LOGGER = logging.getLogger(__name__)

RATE_LIMIT_HEADERS: tuple[str, ...] = ("X-RateLimit-Reset", "RateLimit-Reset", "X-Rate-Limit-Reset")
"""Header names which are often used to indicate how long to wait for a rate limited endpoint."""


@dataclass(kw_only=True, slots=True, frozen=True)
class HTTPRetryConfig:
    status_code: HTTPStatus
    retry_count: int = 3
    backoff_multiplier: int = 1


class HTTPAPIRequestHandle(Handle[Response, JSON_DICT]):
    path: str
    method: HTTPMethod

    def __init__(
        self,
        base_url: URL | str,
        client: AsyncClient,
        retry_config: HTTPRetryConfig | tuple[HTTPRetryConfig, ...] = (),
    ) -> None:
        self.client = client
        self.base_url = URL(base_url) if isinstance(base_url, str) else base_url
        self.retry_config_map: dict[int, HTTPRetryConfig]
        if isinstance(retry_config, HTTPRetryConfig):
            self.retry_config_map = {retry_config.status_code.value: retry_config}
        else:
            self.retry_config_map = {x.status_code.value: x for x in retry_config}

    async def pre_hook(self) -> None:
        pass

    async def post_hook(self, response: Response) -> JSON_DICT:
        value: JSON_DICT = response.json()
        return value

    async def handle(  # noqa: PLR0912
        self,
        query_params: dict | None = None,
        payload: JSON_DICT | None = None,
        path_args: dict[str, str] | None = None,
        headers: dict | None = None,
    ) -> Response:
        request_url = (
            self.base_url.join(self.path) if path_args is None else self.base_url.join(self.path.format(**path_args))
        )

        headers = headers if headers is not None else {}

        try:
            response = await self.client.request(
                method=self.method.value,
                url=request_url,
                headers=headers,
                json=payload,
                params=query_params,
            )
        except TransportError as e:
            LOGGER.info("Request to '%s' failed with: %s", self.base_url, e)
            raise

        # If there were any rate-limit headers, parse them, wait the appropriate time, and then retry the request.
        response_headers = response.headers
        found_rate_limit_headers = (response_headers.get(x) for x in RATE_LIMIT_HEADERS)
        rate_limit: str | int | None = next((x for x in found_rate_limit_headers if x), None)

        if rate_limit and rate_limit != 0:
            try:
                converted_rate_limit = float(rate_limit)
            except (ValueError, TypeError):
                LOGGER.warning("Ignored invalid rate limit value: `%s`", rate_limit)
            else:
                wait = parse_rate_limit(converted_rate_limit)
                LOGGER.debug("Sleeping for %s seconds to honor Rate-Limit headers", wait)
                await trio.sleep(wait)
                return await self.handle(
                    query_params=query_params,
                    payload=payload,
                    path_args=path_args,
                    headers=headers,
                )

        # Retry based on response content is a heuristic; it works around a known issue with authorization failure in
        # Auth0 and possibly other services. For this reason it implements it's own fallback strategy with it's own
        # retry count which is independent of other configuration variables.
        if response.status_code == 401 and has_retry_text(response.text):
            for i in range(3):
                wait = 0.5 * (2 ** (i - 1))  # Following a sane default retry algorithm
                await trio.sleep(wait)
                LOGGER.debug("Sleeping for %s seconds to retry authentication.", wait)
                response = await self.client.request(
                    method=self.method.value,
                    url=request_url,
                    headers=headers,
                    json=payload,
                    params=query_params,
                )
                if response.status_code == 401 and has_retry_text(response.text):
                    continue
                else:
                    return response
            # If we haven't returned yet, just give up and return whatever response we have
            return response

        if retry_config := self.retry_config_map.get(response.status_code):
            backoff = retry_config.backoff_multiplier
            for i in range(retry_config.retry_count):
                wait = backoff * (2 ** (i - 1))
                await trio.sleep(wait)
                response = await self.client.request(
                    method=self.method.value,
                    url=request_url,
                    headers=headers,
                    json=payload,
                    params=query_params,
                )
                if response.status_code != retry_config.status_code:
                    return response
        return response
