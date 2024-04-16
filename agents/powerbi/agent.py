import logging
import sys

from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.configuration.authentication import (
    AzureBasicOauthConfiguration,
    AzureServicePrincipalConfiguration,
)
from framework.core.loops import ChannelReceiveLoop, PeriodicLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from registry.configuration_auth_credentials import load_agent_credentials
from toolkit.more_typing import JSON_DICT

from .config import PowerBIConfiguration
from .constants import COMPONENT_TOOL
from .tasks import PowerBIFetchDatasetsTask

LOGGER = logging.getLogger(__name__)


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("powerbi", PowerBIConfiguration)
    agent_config = registry.lookup("powerbi", PowerBIConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)
    auth_config = load_agent_credentials()
    if not isinstance(auth_config, AzureServicePrincipalConfiguration | AzureBasicOauthConfiguration):
        LOGGER.error(
            "PowerBI agent only supports basic and Azure Service Principal authentication. See docs for more help.",
        )
        sys.exit()
    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with open_nursery() as n:
        n.start_soon(
            PeriodicLoop(
                # PowerBI runs, i.e. datasets, are not expected to change often; hence polling in longer interval.
                period=agent_config.datasets_fetching_period,
                task=PowerBIFetchDatasetsTask(
                    nursery=n,
                    outbound_channel=event_queue_send,
                ),
            ).run,
        )
        n.start_soon(ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run)
        n.start_soon(create_heartbeat_loop(tool=COMPONENT_TOOL), name="Heartbeat")
