import logging
from datetime import UTC, datetime
from http import HTTPMethod, HTTPStatus
from types import TracebackType
from typing import Self, cast

from httpx import Response

from framework import state_store
from framework.authenticators import TokenAuth
from framework.configuration import CoreConfiguration
from framework.configuration.http import ObservabilityHTTPClientConfig
from framework.core.channels import NullSendChannel
from framework.core.handles import HTTPAPIRequestHandle, get_client
from framework.core.tasks import ChannelTask
from framework.observability.events import EVENT_TYPE_KEY
from framework.observability.helpers import handle_observability_exception
from registry.configuration_registry import ConfigurationRegistry
from toolkit.constants import OBSERVABILITY_SERVICE_ACCOUNT_KEY_HEADER_NAME
from toolkit.more_typing import JSON_DICT

LOGGER = logging.getLogger(__name__)


class ObservabilityPostEventHandle(HTTPAPIRequestHandle):
    path = "events/v1/{event_type}"
    method = HTTPMethod.POST

    async def post_hook(self, response: Response) -> JSON_DICT:
        if response.status_code == HTTPStatus.BAD_REQUEST:
            LOGGER.error("Error posting event to Observability: %r", response.json())
        return {}


class EventSenderTask(ChannelTask[JSON_DICT]):
    def __init__(self) -> None:
        super().__init__(outbound_channel=NullSendChannel())
        registry = ConfigurationRegistry()
        self.configuration = registry.lookup("core", CoreConfiguration)
        self.http_config = registry.mutate(
            "observability",
            ObservabilityHTTPClientConfig,
            auth=TokenAuth(
                self.configuration.observability_service_account_key.get_secret_value(),
                header_name=OBSERVABILITY_SERVICE_ACCOUNT_KEY_HEADER_NAME,
                token_prefix="",
            ),
        )
        self.client = get_client(config=self.http_config)
        self.request = ObservabilityPostEventHandle(
            base_url=str(self.configuration.observability_base_url),
            client=self.client,
        )

    async def __aenter__(self) -> Self:
        await self.client.__aenter__()
        return await super().__aenter__()

    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        await self.client.__aexit__(exc_type, exc_val, exc_tb)
        await super().__aexit__(exc_type, exc_val, exc_tb)

    @handle_observability_exception
    async def execute(self, event: JSON_DICT) -> None:
        if EVENT_TYPE_KEY not in event:
            LOGGER.error("Event received cannot be routed, missing %s key", EVENT_TYPE_KEY)
            return
        path_args = {"event_type": cast(str, event.pop(EVENT_TYPE_KEY))}
        result = await self.request.handle(payload=event, path_args=path_args)
        await self.request.post_hook(result)
        result.raise_for_status()
        LOGGER.info("Event %s sent, %d received", path_args["event_type"], result.status_code)
        state_store.latest_event_timestamp = datetime.now(tz=UTC)
