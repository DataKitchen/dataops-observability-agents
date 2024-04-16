from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, List, Optional

from attrs import define, field, validators

from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.status import Status

logger = logging.getLogger(__name__)


@define(kw_only=True)
class AbstractRun(ABC):
    """
    Every plugin must include a subclass of this abstract base class. It represents a
    single pipeline run within the tool this plugin integrates. Let's take Airflow as an
    example. A pipeline run in Airflow is known as a DAG Run. Therefore, a plugin for integrating
    Airflow could include a DAGRun class that inherits from this AbstractRun class. This subclass
    must implement an :meth:`~agents.pollers.abstract_run.AbstractRun.update` method that publishes
    events when the pipeline run status changes (e.g. a task within the DAG Run starts or stops,
    the pipeline itself finishes processing, etc.).

    Parameters
    ----------
    events_publisher: EventPublisher
        Helper class instance for publishing events to the `Events Ingestions API
        <https://api.docs.datakitchen.io/production/events.html>`_.
    pipeline_key: str
        Pipeline key to be passed to the Events Ingestion API endpoints
    run_key: str
        Run key to be passed to the Events Ingestion API endpoints
    finished: bool, optional
        True if the run has completed, either successfully or not. False otherwise (default is True)
    """

    events_publisher: EventsPublisher = field(validator=validators.instance_of(EventsPublisher))
    """Publishes events to the Events Ingestion API"""
    pipeline_key: str = field(validator=[validators.instance_of(str), validators.max_len(255)])  # type: ignore
    """Pipeline key passed to the Events Ingestion API"""
    run_key: str = field(validator=[validators.instance_of(str), validators.max_len(255)])  # type: ignore
    """Run key passed to the Events Ingestion API"""
    finished: bool = field(default=False, validator=validators.instance_of(bool))
    """True if the run has completed, either successfully or not. False otherwise (default is True)"""

    agent_key: str = field(validator=validators.instance_of(str))
    """ Must be set when object is created to allow for heartbeats to be sent to the correct agent component"""
    agent_name: str = field(validator=validators.instance_of(str))
    """ Must be set when object is created to allow for heartbeats to be sent to the correct agent component"""

    @abstractmethod
    def update(self) -> None:
        """
        This method is invoked on every polling interval. Any state change that has occurred since
        the last polling interval (e.g. a new task started, the run finished processing, etc.)
        should publish an appropriate event using the
        :meth:`~agents.pollers.abstract_run.AbstractRun.publish_run_status_event` helper method.

        Returns
        -------
        None
        """
        raise NotImplementedError

    def publish_run_status_event(
        self,
        event_timestamp: datetime,
        task_key: Optional[str],
        status: Status,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
        component_tool: Optional[str] = None,
    ) -> None:
        """
        Forward task event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_run_status_event`
        """
        return self.events_publisher.publish_run_status_event(
            event_timestamp,
            self.pipeline_key,
            self.run_key,
            task_key,
            status,
            pipeline_name,
            task_name,
            metadata,
            external_url,
            component_tool,
        )

    def publish_metric_log_event(
        self,
        event_timestamp: datetime,
        metric_key: str,
        metric_value: float,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        task_key: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
        component_tool: Optional[str] = None,
    ) -> None:
        """
        Forward metric log event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_metric_log_event`
        """
        return self.events_publisher.publish_metric_log_event(
            event_timestamp,
            self.pipeline_key,
            self.run_key,
            task_key,
            metric_key,
            metric_value,
            pipeline_name,
            task_name,
            metadata,
            external_url,
            component_tool,
        )

    def publish_message_log_event(
        self,
        event_timestamp: datetime,
        task_key: Optional[str],
        log_level: MessageEventLogLevel,
        message: str,
        pipeline_name: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
        component_tool: Optional[str] = None,
    ) -> None:
        """
        Forward message log event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_message_log_event`
        """
        return self.events_publisher.publish_message_log_event(
            event_timestamp,
            self.pipeline_key,
            self.run_key,
            task_key,
            log_level,
            message,
            pipeline_name,
            task_name,
            metadata,
            external_url,
            component_tool,
        )

    # 2023-10-04 ECE: Introducing Dataset as a concept here. Runs and Datasets need to be refactored out, but with
    # the new framework on it's way this isn't a good use of time.
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
            event_timestamp=event_timestamp,
            dataset_key=dataset_key,
            dataset_name=dataset_name,
            operation=operation,
            path=path,
            metadata=metadata,
            external_url=external_url,
            pipeline_key=pipeline_key,
            pipeline_name=pipeline_name,
            stream_key=stream_key,
            stream_name=stream_name,
            server_key=server_key,
            server_name=server_name,
            run_key=run_key,
            run_name=run_name,
            task_key=task_key,
            task_name=task_name,
            component_tool=component_tool,
        )

    # 2023-10-04 ECE: Added to handle test outcomes that are for dataset events. This class has test outcomes
    # for pipelines.
    # This class should be refactored. We need an Run and Dataset versions of what is now AbstractRun.
    # Doing the refactoring with the new framework imminent is not a good use of time.
    def publish_test_outcomes_event_dataset(
        self,
        event_timestamp: datetime,
        dataset_key: str,
        test_outcomes: List[dict],
        dataset_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
    ) -> None:
        """
        Forward test outcome event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_test_outcomes_event`
        """
        return self.events_publisher.publish_test_outcomes_event_dataset(
            event_timestamp=event_timestamp,
            dataset_key=dataset_key,
            test_outcomes=test_outcomes,
            dataset_name=dataset_name,
            metadata=metadata,
            external_url=external_url,
            component_tool=self.component_tool,
        )

    # This is for pipeline test outcomes
    def publish_test_outcomes_event(
        self,
        event_timestamp: datetime,
        test_outcomes: List[dict],
        pipeline_name: Optional[str] = None,
        task_key: Optional[str] = None,
        task_name: Optional[str] = None,
        metadata: Any = None,
        external_url: Optional[str] = None,
    ) -> None:
        """
        Forward test outcome event publishing to
        :meth:`~agents.common.events_publisher.EventsPublisher.publish_test_outcomes_event`
        """
        return self.events_publisher.publish_test_outcomes_event(
            event_timestamp,
            self.pipeline_key,
            self.run_key,
            test_outcomes,
            task_key,
            pipeline_name,
            task_name,
            metadata,
            external_url,
        )

    # AWS SQS (sqs)
    # Azure Blob Storage (blob_storage)
    # Azure Functions (azure_functions)
    # Fivetran Log Connector (fivetran)
    # GoogleCloudComposer (gcc)
    # Talend (talend)
    @property
    def component_tool(self) -> str:
        raise NotImplementedError
