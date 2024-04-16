from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Optional

from attrs import define, field, validators

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status

logger = logging.getLogger(__name__)


@define(kw_only=True, slots=False)
class AbstractEventHandler(ABC):
    """
    Every plugin must include a subclass of this abstract base class. The subclass must handle event
    records coming from Event Hubs. It's the responsibility of the subclass to parse the record,
    manage state, and publish events accordingly.
    """

    plugins = []  # type: ignore
    """List of subclasses of :class:`~agents.pollers.abstract_runs_fetcher.AbstractRunsFetcher`"""
    events_publisher: EventsPublisher = field(validator=validators.instance_of(EventsPublisher))
    """Publishes events to the Events Ingestion API"""

    def __init_subclass__(cls, **kwargs) -> None:  # type: ignore
        super().__init_subclass__(**kwargs)
        cls.plugins.append(cls)

    @classmethod
    @abstractmethod
    def create_event_handler(cls, events_publisher: EventsPublisher) -> AbstractEventHandler:
        """
        Plugins must include a subclass of AbstractEventHandler. The subclass must
        implement this method for creating and returning an instance of the subclass. This method
        provides the plugin with a means to instantiate the class with whatever arguments
        it requires.

        Parameters
        ----------
        events_publisher: EventPublisher
            Helper class instance for publishing events to the `Events Ingestions API
            <https://api.docs.datakitchen.io/production/events.html>`_.

        Returns
        -------
        AbstractEventHandler
            The returned instance must be a subclass of AbstractEventHandler.
        """
        raise NotImplementedError

    @abstractmethod
    def handle_event_record(self, event_record: dict) -> bool:
        """
        Return False if the event is NOT handled by this handler. This indicates that other plugins
        should attempt to handle this event. Otherwise, handle the event and return True to indicate
        that the event needs no further processing by other plugins.
        """
        raise NotImplementedError

    def publish_run_status_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: Optional[str],
        status: Status,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
    ) -> None:
        """
        Forward run event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_run_status_event`
        """
        return self.events_publisher.publish_run_status_event(
            event_timestamp,
            pipeline_key,
            run_key,
            task_key,
            status,
            pipeline_name,
            task_name,
            metadata,
            external_url,
        )

    def publish_metric_log_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: Optional[str],
        metric_key: str,
        metric_value: float,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
    ) -> None:
        """
        Forward metric log event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_metric_log_event`
        """
        return self.events_publisher.publish_metric_log_event(
            event_timestamp,
            pipeline_key,
            run_key,
            task_key,
            metric_key,
            metric_value,
            pipeline_name,
            task_name,
            metadata,
            external_url,
        )

    def publish_message_log_event(
        self,
        event_timestamp: datetime,
        pipeline_key: str,
        run_key: str,
        task_key: Optional[str],
        log_level: MessageEventLogLevel,
        message: str,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
    ) -> None:
        """
        Forward message log event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_message_log_event`
        """
        return self.events_publisher.publish_message_log_event(
            event_timestamp,
            pipeline_key,
            run_key,
            task_key,
            log_level,
            message,
            pipeline_name,
            task_name,
            metadata,
            external_url,
        )

    def publish_dataset_event(
        self,
        event_timestamp: datetime,
        dataset_key: str,
        dataset_name: Optional[str],
        operation: str,
        path: Optional[str],
        metadata: Any = None,
        external_url: Optional[str] = None,
        pipeline_key: Optional[str] = None,
        pipeline_name: Optional[str] = None,
        stream_key: Optional[str] = None,
        stream_name: Optional[str] = None,
        server_key: Optional[str] = None,
        server_name: Optional[str] = None,
        run_key: Optional[str] = None,
        run_name: Optional[str] = None,
        task_key: Optional[str] = None,
        task_name: Optional[str] = None,
        component_tool: Optional[str] = None,
    ) -> None:
        """
        Forward dataset event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_dataset_event`
        """
        return self.events_publisher.publish_dataset_event(
            event_timestamp,
            dataset_key,
            dataset_name,
            operation,
            path,
            metadata,
            external_url,
            pipeline_key,
            pipeline_name,
            stream_key,
            stream_name,
            server_key,
            server_name,
            run_key,
            run_name,
            task_key,
            task_name,
            component_tool,
        )
