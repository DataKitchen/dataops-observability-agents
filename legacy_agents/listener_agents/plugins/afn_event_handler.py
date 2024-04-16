import logging
from datetime import datetime
from typing import Optional, Tuple

from attrs import define
from dateutil import parser

from common.events_publisher import EventsPublisher
from common.status import Status
from listener_agents.abstract_event_handler import AbstractEventHandler

logger = logging.getLogger(__name__)

VALID_CATEGORIES = ["FunctionAppLogs"]
VALID_OPERATIONS = ["Microsoft.Web/sites/functions/log"]
VALID_EVENTS = ["FunctionStarted", "FunctionCompleted"]


@define(kw_only=True, slots=False)
class AFNEventHandler(AbstractEventHandler):
    @classmethod
    def create_event_handler(cls, events_publisher: EventsPublisher) -> AbstractEventHandler:
        return AFNEventHandler(events_publisher=events_publisher)

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _get_category(self, event_record: dict) -> Optional[str]:
        if "category" in event_record:
            category = str(event_record["category"])
            if category in VALID_CATEGORIES:
                return category
            else:
                logger.debug(f"Invalid category found by {self.name}: {category}")
        logger.debug(f"Category not in event_record")
        return None

    def _get_event_name(self, event_properties: dict) -> Optional[str]:
        if "eventName" in event_properties:
            event_name = str(event_properties["eventName"])
            if event_name in VALID_EVENTS:
                return event_name
            else:
                logger.debug(f"Invalid event name found by {self.name}: {event_name}")
        logger.debug(f"event_name not in event_record")
        return None

    @staticmethod
    def _get_status(event_properties: dict) -> Status:
        status = event_properties["eventName"]
        level = event_properties["level"]
        if status == "FunctionStarted":
            return Status.RUNNING
        elif status == "FunctionCompleted":
            if level == "Error":
                return Status.FAILED
            else:
                return Status.COMPLETED
        else:
            logger.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
            return Status.UNKNOWN

    def handle_event_record(self, event_record: dict) -> bool:
        """
        Return False if the event is NOT handled by this handler. This indicates that other plugins
        should attempt to handle this event. Otherwise, handle the event and return True to indicate
        that the event needs no further processing by other plugins.
        """
        category = self._get_category(event_record)

        if category is None:
            return False

        if "properties" in event_record:
            metadata = event_record["properties"]
        else:
            logger.info(f"No properties found.")
            return False

        event_name = self._get_event_name(metadata)
        if event_name is None:
            return False

        logger.info(f"Processing Event from Azure Function - Log Event Record: {event_record}")

        status = self._get_status(metadata)
        if status == Status.UNKNOWN:
            return True

        event_timestamp: Optional[datetime] = parser.parse(event_record["time"])
        if event_timestamp is None:
            return False

        if "appName" not in metadata:
            logger.info(f"appName not in event record. Event Record {metadata}")
            return False

        pipeline_key = metadata["appName"]
        task_name = pipeline_key

        if "functionInvocationId" in metadata:
            run_key = metadata["functionInvocationId"]
            task_key = metadata["functionInvocationId"]

        if status == Status.RUNNING:
            logger.info(f"Publishing Start event Log Status:  {status}")
            # Start run
            self.publish_run_status_event(
                event_timestamp, pipeline_key, run_key, None, status, None, None, metadata, None
            )
            # Start task
            self.publish_run_status_event(
                event_timestamp,
                pipeline_key,
                run_key,
                task_key,
                status,
                None,
                task_name,
                metadata,
                None,
            )

        if status.finished:
            logger.info(f"Publishing Stop event Log Status:  {status}")
            # Close task

            self.publish_run_status_event(
                event_timestamp,
                pipeline_key,
                run_key,
                task_key,
                status,
                None,
                task_name,
                metadata,
                None,
            )
            # Close run
            self.publish_run_status_event(
                event_timestamp, pipeline_key, run_key, None, status, None, None, metadata
            )

        else:
            logger.error(f"Unrecognized category for {self.name}: {category}")
        return True
