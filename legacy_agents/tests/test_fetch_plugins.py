import os

import pytest

from common.plugin_utils import fetch_plugins
from listener_agents import event_hubs_agent
from listener_agents.abstract_event_handler import AbstractEventHandler
from poller_agents import poller
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher


@pytest.mark.unit
def test_fetch_listener_plugins():
    assert len(AbstractEventHandler.plugins) == 0
    os.environ["ENABLED_PLUGINS"] = "afn_event_handler"
    fetch_plugins(AbstractEventHandler, event_hubs_agent.PLUGINS_PATHS)
    assert len(AbstractEventHandler.plugins) > 0

    # Reset plugins to empty for other tests
    AbstractEventHandler.plugins = []


@pytest.mark.unit
def test_fetch_poller_plugins():
    assert len(AbstractRunsFetcher.plugins) == 0
    os.environ["ENABLED_PLUGINS"] = "airflow_composer"
    fetch_plugins(AbstractRunsFetcher, poller.PLUGINS_PATHS)
    assert len(AbstractRunsFetcher.plugins) > 0

    # Reset plugins to empty for other tests
    AbstractRunsFetcher.plugins = []
