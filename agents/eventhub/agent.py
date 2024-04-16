from trio import open_memory_channel, open_nursery

from framework.configuration import CoreConfiguration
from framework.core.loops import ChannelReceiveLoop
from framework.observability import EventSenderTask, create_heartbeat_loop
from registry import ConfigurationRegistry
from toolkit.more_typing import JSON_DICT

from .configuration import EventhubConfiguration
from .constants import COMPONENT_TOOL
from .loop import EventHubLoop
from .tasks import EventHubReceiveTask


async def main() -> None:
    registry = ConfigurationRegistry()
    registry.register("eventhubs", EventhubConfiguration)
    core_config = registry.lookup("core", CoreConfiguration)

    max_channel_capacity = int(core_config.max_channel_capacity)
    event_queue_send, event_queue_receive = open_memory_channel[JSON_DICT](max_channel_capacity)
    async with open_nursery() as n:
        n.start_soon(
            EventHubLoop(
                task=EventHubReceiveTask(outbound_channel=event_queue_send),
            ).run,
        )
        n.start_soon(ChannelReceiveLoop(inbound_channel=event_queue_receive, task=EventSenderTask()).run)
        n.start_soon(create_heartbeat_loop(tool=COMPONENT_TOOL), name="Eventhub-Heartbeat")
