import logging

from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.core.loops import ChannelReceiveLoop, PeriodicLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .configuration import DatabricksConfiguration
from .constants import COMPONENT_TOOL
from .tasks import DatabricksListRunsTask

LOGGER = logging.getLogger(__name__)


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("databricks", DatabricksConfiguration)
    agent_config = registry.lookup("databricks", DatabricksConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)

    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with open_nursery() as n:
        n.start_soon(
            PeriodicLoop(
                period=agent_config.period,
                task=DatabricksListRunsTask(nursery=n, outbound_channel=event_queue_send),
            ).run,
        )
        n.start_soon(ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run)
        n.start_soon(create_heartbeat_loop(tool=COMPONENT_TOOL), name="Heartbeat")
