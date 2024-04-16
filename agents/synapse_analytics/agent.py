import logging

from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.configuration.authentication import (
    AzureServicePrincipalConfiguration,
)
from framework.core.loops import ChannelReceiveLoop, PeriodicLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .client import artifacts_client
from .config import SynapseAnalyticsConfiguration
from .constants import COMPONENT_TOOL
from .list_runs_task import ListRunsTask

LOGGER = logging.getLogger(__name__)


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("synapse_analytics", SynapseAnalyticsConfiguration)
    registry.register("auth_azure_spn", AzureServicePrincipalConfiguration)
    agent_config = registry.lookup("synapse_analytics", SynapseAnalyticsConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)
    if not agent_config.workspace_id:
        LOGGER.warning(
            "Synapse subscription ID and resource group name are not configured. No Synapse URLs will be generated",
        )

    max_channel_capacity = core_config.max_channel_capacity
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with artifacts_client, open_nursery() as nursery:
        nursery.start_soon(
            PeriodicLoop(
                period=agent_config.period,
                task=ListRunsTask(nursery=nursery, outbound_channel=event_queue_send),
            ).run,
            name="ListRuns",
        )
        nursery.start_soon(
            ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run,
            name="EventSender",
        )
        nursery.start_soon(create_heartbeat_loop(tool=COMPONENT_TOOL), name="Heartbeat")
