import logging
from abc import ABC, abstractmethod
from collections.abc import Container
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal, cast

from dateutil import parser

from toolkit.more_typing import JSON_DICT
from toolkit.observability import EVENT_TYPE_KEY, EventType, Status

from .constants import COMPONENT_TOOL

LOG = logging.getLogger(__name__)

"""
This file holds Parsers which can be used to determine is an event-hub message is something we can process, and
handle said processing.
"""


def coalesce(data: JSON_DICT, *keys: str) -> Any:
    for k in keys:
        if (d := data.get(k)) is not None:
            return d
    # this should be unreachable if a Parser's applies is called first
    raise ValueError(f"No data for key {keys}")


def translate_eventhubs_status(status: str) -> Status:
    if status == "InProgress":
        return Status.RUNNING
    elif status == "Succeeded":
        return Status.COMPLETED
    elif status == "Failed":
        return Status.FAILED
    else:
        LOG.error("Unrecognized status: %s. Setting status to UNKNOWN", status)
        return Status.UNKNOWN


@dataclass(frozen=True, slots=True)
class Keys:
    """
    When we process a message, we have to detect it via its various keys; some keys are optionally-required,
    so this class allows us to specify relationships in the Parser.
    """

    names: tuple[str, ...]
    """Key names that we're concerned with."""
    relation: Literal["AllOf", "OneOf", "NoneOf"]
    """
    The relation between keys.
    """

    def valid(self, event_data: Container) -> bool:
        if self.relation == "AllOf":
            return all(name in event_data for name in self.names)
        elif self.relation == "OneOf":
            return any(name in event_data for name in self.names)
        else:
            # "NoneOf"
            return not any(name in event_data for name in self.names)


class EventHubBaseParser(ABC):
    valid_categories: frozenset[str]
    keys: frozenset[Keys]
    """
    This is the structural type for a Parser.
    """

    async def applies(self, event_record: JSON_DICT) -> bool:
        if "category" not in event_record:
            return False
        if event_record["category"] not in self.valid_categories:
            return False

        return all(key.valid(event_record) for key in self.keys)

    @abstractmethod
    async def publish(self, event_data: JSON_DICT) -> list[JSON_DICT]:
        raise NotImplementedError()


class UnknownStatusParser(EventHubBaseParser):

    """
    This handler is just here to weed out events without status.
    """

    def __init__(self) -> None:
        """Mypy demands this here, for some reason, even when its a dataclass."""
        self.valid_categories = frozenset()
        self.keys = frozenset()

    async def applies(self, event_record: JSON_DICT) -> bool:
        return translate_eventhubs_status(cast(str, event_record.get("status", ""))) == Status.UNKNOWN

    async def publish(self, event_data: JSON_DICT) -> list[JSON_DICT]:
        return cast(list[JSON_DICT], [])


@dataclass(frozen=True, slots=True)
class ADFParser(EventHubBaseParser):
    valid_categories: frozenset[str] = frozenset(("ActivityRuns", "PipelineRuns"))
    """
    Check if it has a status field. Use None to skip check.
    """
    keys: frozenset[Keys] = frozenset(
        (
            Keys(("pipelineName", "resourceId", "status"), relation="AllOf"),
            Keys(("pipelineRunId", "runId"), relation="OneOf"),
            Keys(("start", "end"), relation="OneOf"),
        ),
    )
    """
    Allows us to specify AND relationships between the keys
    """

    async def applies(self, event_record: JSON_DICT) -> bool:
        return all(
            (
                await EventHubBaseParser.applies(self, event_record),
                translate_eventhubs_status(cast(str, event_record.get("status", ""))) != Status.UNKNOWN,
            ),
        )

    async def publish(self, event_record: JSON_DICT) -> list[JSON_DICT]:
        pipeline_key = cast(str, event_record["pipelineName"])
        run_key = cast(str, coalesce(event_record, "pipelineRunId", "runId"))
        event_timestamp: datetime = parser.parse(cast(str, event_record["timestamp"]))
        metadata = event_record.get("properties")
        activity_type = event_record.get("activityType")
        task_key = event_record.get("activityName")
        resource_id = event_record["resourceId"]
        external_url = f"https://adf.azure.com/monitoring/pipelineruns/{run_key}?factory={resource_id}"
        status = translate_eventhubs_status(cast(str, event_record["status"]))
        output = cast(dict, event_record.get("properties", {})).get("Output")

        events = [
            {
                EVENT_TYPE_KEY: EventType.RUN_STATUS.value,
                "event_timestamp": event_timestamp.astimezone().isoformat(),
                "pipeline_key": pipeline_key,
                "run_key": run_key,
                "task_key": task_key,
                "status": status.name,
                "pipeline_name": None,
                "task_name": None,
                "metadata": metadata,
                "external_url": external_url,
                "component_tool": COMPONENT_TOOL,
            },
        ]

        if activity_type == "Copy" and output is not None:
            metric_keys = {"filesRead", "filesWritten", "dataRead", "dataWritten"}
            for metric_key in metric_keys & output.keys():
                metric_value = output[metric_key]

                events.append(
                    {
                        EVENT_TYPE_KEY: EventType.METRIC_LOG.value,
                        "event_timestamp": event_timestamp.astimezone().isoformat(),
                        "pipeline_key": pipeline_key,
                        "run_key": run_key,
                        "task_key": task_key,
                        "metric_key": metric_key,
                        "metric_value": metric_value,
                        "pipeline_name": None,
                        "task_name": None,
                        "metadata": metadata,
                        "external_url": external_url,
                        "component_tool": COMPONENT_TOOL,
                    },
                )

        return events
