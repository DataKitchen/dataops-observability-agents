from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from attrs import define, field, validators

from common.events_publisher import EventsPublisher
from poller_agents.abstract_run import AbstractRun

logger = logging.getLogger(__name__)


@define(kw_only=True, slots=False)
class AbstractRunsFetcher(ABC):
    """
    Every plugin must include a subclass of this abstract base class. This subclass
    must fetch runs from the tool that the plugin is integrating. In addition, the plugin
    must implement a subclass of :class:`~agents.pollers.abstract_run.AbstractRun`. Consequently,
    the runs fetched by the
    :meth:`~agents.pollers.abstract_runs_fetcher.AbstractRunsFetcher.fetch_runs` method must return
    a list of the subclassed :class:`~agents.pollers.abstract_run.AbstractRun` objects.
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
    def create_runs_fetcher(cls, events_publisher: EventsPublisher) -> AbstractRunsFetcher:
        """
        Plugins must include a subclass of AbstractRunsFetcher. The subclass must
        implement this method for creating and returning an instance of the subclass. This method
        provides the plugin with a means to instantiate the class with whatever arguments
        it requires.

        Parameters
        ----------
        events_publisher: EventPublisher
            Helper class instance for publishing events to the `Events Ingestion API
            <https://api.docs.datakitchen.io/production/events.html>`_.

        Returns
        -------
        AbstractRunsFetcher
            The returned instance must be a subclass of AbstractRunsFetcher.
        """
        raise NotImplementedError

    @abstractmethod
    def fetch_runs(
        self, execution_date_gte: datetime, execution_date_lte: datetime
    ) -> List[AbstractRun]:
        """
        Plugins must include a subclass of
        :class:`~agents.pollers.abstract_runs_fetcher.AbstractRunsFetcher`. The subclass must
        implement this method for fetching runs that started within the time window represented by
        ``execution_date_gte`` and ``execution_date_lte``. For instance, Airflow has the notion of
        DAG runs. Therefore, an Airflow plugin would return a list of DAG runs that started within
        the provided time window. An example AirflowRunsFetcher class is provided in
        airflow.py in the plugins directory.

        In addition, every plugin must include a subclass of
        :class:`~agents.pollers.abstract_run.AbstractRun`. In the case of Airflow, this subclass is
        named AirflowRun (see the example AirflowRun class defined in airflow.py in the plugins
        directory). The return value of fetch_runs should be a list of instances of the
        :class:`~agents.pollers.abstract_run.AbstractRun` subclass.

        Parameters
        ----------
        execution_date_gte: datetime
            Fetch runs that started after this timestamp.
        execution_date_lte: datetime
            Fetch runs that started before this timestamp.

        Returns
        -------
        List[AbstractRun]
            List of :class:`~agents.pollers.abstract_run.AbstractRun` subclass objects.
        """
        raise NotImplementedError

    @property
    def component_tool(self) -> str:
        raise NotImplementedError

    @property
    def agent_name(self) -> str:
        raise NotImplementedError

    @property
    def agent_key(self) -> str:
        raise NotImplementedError
