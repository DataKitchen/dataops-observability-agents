import logging
from collections.abc import Iterable

from azure.eventhub import EventData
from trio import MemorySendChannel

from framework.core.tasks import Task
from registry import ConfigurationRegistry

from .configuration import EventhubConfiguration
from .parsers import ADFParser, EventHubBaseParser, UnknownStatusParser

LOG = logging.getLogger(__name__)


class EventHubReceiveTask(Task):
    def __init__(self, outbound_channel: MemorySendChannel) -> None:
        super().__init__(outbound_channel=outbound_channel)
        configuration: EventhubConfiguration = ConfigurationRegistry().lookup("eventhubs", EventhubConfiguration)
        self.message_types: set[EventHubBaseParser] = set()
        self.message_types.add(UnknownStatusParser())
        # default parser
        for m_type in configuration.message_types:
            if m_type == "ADF":
                self.message_types.add(ADFParser())
            else:
                raise NotImplementedError(f"Unknown message type {m_type}")

    async def execute(self, events: None | EventData | Iterable[EventData]) -> None:
        #  Reading the library code, It's apparently possible to receive None, an EventData object, or a list of EventData, which we normalize to be a list of EventData. Internally, each EventData may contain multiple records.

        if events is None:
            return
        elif isinstance(events, EventData):
            events = [events]

        for event in events:
            try:
                event_data = event.body_as_json(encoding="UTF-8")
            except TypeError:
                LOG.warning("Could not retrieve event as JSON. message_id = %s", event.message_id)
                continue

            for record in event_data["records"]:
                LOG.info(
                    "Processing event: %s",
                    [f"{k} = {record.get(k, '#')}" for k in ["category", "pipelineName", "status", " runId"]],
                )
                for parser in self.message_types:
                    if await parser.applies(record):
                        try:
                            obs_events = await parser.publish(record)
                            for obs_event in obs_events:
                                await self.outbound_channel.send(obs_event)
                        except Exception:
                            LOG.exception("Error processing record: %r", record)
                            continue
