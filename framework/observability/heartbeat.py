import logging
from collections.abc import Awaitable, Callable
from datetime import datetime
from http import HTTPMethod

from httpx import URL

from framework import __version__, state_store
from framework.authenticators import TokenAuth
from framework.configuration import CoreConfiguration
from framework.configuration.http import ObservabilityHTTPClientConfig
from framework.core.handles import HTTPAPIRequestHandle, get_client
from framework.core.loops import PeriodicLoop
from framework.core.tasks import PeriodicTask
from framework.observability.helpers import handle_observability_exception
from registry import ConfigurationRegistry
from toolkit.constants import OBSERVABILITY_SERVICE_ACCOUNT_KEY_HEADER_NAME

LOGGER = logging.getLogger(__name__)


class ObservabilityPostHeartbeatHandle(HTTPAPIRequestHandle):
    path = "agent/v1/heartbeat"
    method = HTTPMethod.POST


class HeartbeatTask(PeriodicTask):
    def __init__(self, tool: str) -> None:
        super().__init__()
        core_config = ConfigurationRegistry().lookup("core", CoreConfiguration)
        self.tool = tool
        self.agent_key = core_config.agent_key
        http_config = ConfigurationRegistry().mutate(
            "observability",
            ObservabilityHTTPClientConfig,
            auth=TokenAuth(
                core_config.observability_service_account_key.get_secret_value(),
                header_name=OBSERVABILITY_SERVICE_ACCOUNT_KEY_HEADER_NAME,
                token_prefix="",
            ),
        )
        self.client = get_client(http_config)
        self.handle = ObservabilityPostHeartbeatHandle(
            base_url=URL(str(core_config.observability_base_url)),
            client=self.client,
        )

    @handle_observability_exception
    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        maybe_isoformat = timestamp.isoformat() if (timestamp := state_store.latest_event_timestamp) else None
        result = await self.handle.handle(
            payload={
                "key": self.agent_key,
                "tool": self.tool,
                "latest_event_timestamp": maybe_isoformat,
                "version": __version__,
            },
        )
        result.raise_for_status()
        LOGGER.debug("Heartbeat sent")


def create_heartbeat_loop(tool: str) -> Callable[..., Awaitable[None]]:
    return PeriodicLoop(
        period=ConfigurationRegistry().lookup("core", CoreConfiguration).heartbeat_period,
        task=HeartbeatTask(tool=tool),
    ).run
