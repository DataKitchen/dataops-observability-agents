import logging
from datetime import datetime, timedelta
from typing import Optional, Tuple

from attrs import define
from dateutil import parser

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status
from listener_agents.abstract_event_handler import AbstractEventHandler

logger = logging.getLogger(__name__)

VALID_CATEGORIES = ["StorageWrite", "StorageDelete"]


@define(kw_only=True, slots=False)
class ADFEventHandler(AbstractEventHandler):
    @classmethod
    def create_event_handler(cls, events_publisher: EventsPublisher) -> AbstractEventHandler:
        return ADFEventHandler(events_publisher=events_publisher)

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

    @staticmethod
    def _get_status_and_timestamp(event_record: dict) -> Tuple[Status, Optional[datetime]]:
        status = event_record["statusText"]
        if status == "Success":
            return Status.COMPLETED, parser.parse(event_record["time"])
        elif status == "Failed":
            return Status.FAILED, parser.parse(event_record["time"])
        else:
            logger.error(f"Unrecognized status: {status}. Setting status to {Status.UNKNOWN.name}")
            return Status.UNKNOWN, None

    def handle_event_record(self, event_record: dict) -> bool:
        """
        Return False if the event is NOT handled by this handler. This indicates that other plugins
        should attempt to handle this event. Otherwise, handle the event and return True to indicate
        that the event needs no further processing by other plugins.
        """
        category = self._get_category(event_record)

        if category is None:
            return False

        # logger.info(f"Log Event Record: {event_record}")

        status, event_timestamp = self._get_status_and_timestamp(event_record)
        if status == Status.UNKNOWN:
            return True

        if event_timestamp is None:
            return False

        logger.info(f"Processing Event from Azure BLOB - Log Event Record: {event_record}")

        run_key = event_record["correlationId"]

        metadata = event_record["properties"]

        storage_account_name = metadata["accountName"]
        pipeline_key = storage_account_name + "_" + category

        message = metadata["objectKey"]

        task_key = event_record["operationName"]

        task_name = event_record["operationName"]

        # Send message log event
        self.publish_message_log_event(
            event_timestamp,
            pipeline_key,
            run_key,
            None,
            MessageEventLogLevel.INFO,
            message,
            None,
            None,
            metadata,
            None,
        )
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
            event_timestamp + timedelta(milliseconds=2),
            pipeline_key,
            run_key,
            None,
            status,
            None,
            None,
            metadata,
            None,
        )
        return True
