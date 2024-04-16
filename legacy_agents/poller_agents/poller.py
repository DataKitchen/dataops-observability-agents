import logging
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List

from events_ingestion_client import ApiClient, Configuration, EventsApi

from common.component_helper import ComponentHelper
from common.events_publisher import EventsPublisher
from common.message_event_log_level import MessageEventLogLevel
from common.plugin_utils import fetch_plugins
from poller_agents.abstract_run import AbstractRun
from poller_agents.abstract_runs_fetcher import AbstractRunsFetcher

NATIVE_PLUGINS_PATH: Path = Path(__file__).parent / "plugins"
PLUGINS_PATHS: list[str] = [str(NATIVE_PLUGINS_PATH)]
EXTERNAL_PLUGINS_PATH: str = os.getenv("EXTERNAL_PLUGINS_PATH", "")
if EXTERNAL_PLUGINS_PATH:
    PLUGINS_PATHS.append(EXTERNAL_PLUGINS_PATH)
POLLING_INTERVAL_SECS: int = int(os.getenv("POLLING_INTERVAL_SECS", 10))
MAX_WORKERS: int = int(os.getenv("MAX_WORKERS", 10))
PUBLISH_EVENTS = os.getenv("PUBLISH_EVENTS", "true").lower() in ["true", "1"]

# Heartbeat Internal is in Seconds
heartbeat_interval_seconds: int = 10 * 60
if "HEARTBEAT_INTERVAL" in os.environ:
    heartbeat_interval_seconds = int(str(os.environ.get("HEARTBEAT_INTERVAL")))
elif "HEARTBEAT_INTERNAL" in os.environ:
    heartbeat_interval_seconds = int(str(os.environ.get("HEARTBEAT_INTERNAL")))

agent_heartbeat_prefix: str = str(os.getenv("AGENT_HEARTBEAT_PREFIX", "DK Agent Heartbeat"))
agent_heartbeat_description: str = str(
    os.getenv(
        "AGENT_HEARTBEAT_DESCRIPTION",
        "System managed component to ensure that a given agent is running",
    )
)
# Schedule is in minutes
agent_heartbeat_schedule_seconds: int = int(
    os.getenv("AGENT_HEARTBEAT_SCHEDULE", 12 * 60)
)  # default 12 minutes
agent_heartbeat_grace_period_seconds: int = int(
    os.getenv("AGENT_HEARTBEAT_GRACE_PERIOD", 2 * 60)
)  # default 2 minutes

agent_freshness_prefix: str = str(os.getenv("AGENT_FRESHNESS_PREFIX", "DK Agent Freshness"))
agent_freshness_description: str = str(
    os.getenv(
        "AGENT_FRESHNESS_DESCRIPTION",
        "System managed component to track the last events fetched by a given agent",
    )
)
# Schedule is in minutes. Assume most jobs run every 59 min
agent_freshness_schedule_seconds: int = int(
    os.getenv("AGENT_FRESHNESS_SCHEDULE", 59 * 60)
)  # 59 minutes

agent_freshness_grace_period_seconds: int = int(
    os.getenv("AGENT_FRESHNESS_GRACE_PERIOD", 10 * 60)
)  # default 10 minutes


# Configure logging
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))
logger.setLevel(logging.INFO)

VERIFY_SSL = {"true": True, "false": False}[os.getenv("DK_EVENTS_VERIFY_SSL", "true").lower()]

# ---------------
config_message = "Agent General Configuration:\n"

config_message += f"PLUGINS_PATHS: {';'.join(PLUGINS_PATHS)}\n"
config_message += f"MAX_WORKERS: {MAX_WORKERS}\n"

config_message += f"EXTERNAL_PLUGINS_PATH: {EXTERNAL_PLUGINS_PATH}\n"
config_message += f"POLLING_INTERVAL_SECS: {POLLING_INTERVAL_SECS}\n"
config_message += f"MAX_WORKERS: {MAX_WORKERS}\n"
config_message += f"PUBLISH_EVENTS: {PUBLISH_EVENTS}\n"

config_message += f"HEARTBEAT_INTERVAL (seconds): {heartbeat_interval_seconds}\n"

config_message += f"AGENT_HEARTBEAT_PREFIX: {agent_heartbeat_prefix}\n"
config_message += f"AGENT_HEARTBEAT_DESCRIPTION: {agent_heartbeat_description}\n"
config_message += f"AGENT_HEARTBEAT_SCHEDULE (seconds): {agent_heartbeat_schedule_seconds}\n"
config_message += (
    f"AGENT_HEARTBEAT_GRACE_PERIOD (seconds): {agent_heartbeat_grace_period_seconds}\n"
)

config_message += f"AGENT_FRESHNESS_PREFIX: {agent_freshness_prefix}\n"
config_message += f"AGENT_FRESHNESS_DESCRIPTION: {agent_freshness_description}\n"
config_message += f"AGENT_FRESHNESS_SCHEDULE (seconds): {agent_freshness_schedule_seconds}\n"
config_message += (
    f"AGENT_FRESHNESS_GRACE_PERIOD (seconds): {agent_freshness_grace_period_seconds}\n"
)

config_message += f"DK_EVENTS_VERIFY_SSL: {VERIFY_SSL}\n"

logger.info(config_message)

# - ---------------------

if not VERIFY_SSL:
    import urllib3
    from urllib3 import exceptions

    urllib3.disable_warnings(exceptions.InsecureRequestWarning)


def main() -> None:
    # Configure API key authorization: SAKey
    configuration = Configuration()
    configuration.api_key["ServiceAccountAuthenticationKey"] = os.getenv("EVENTS_API_KEY")
    configuration.host = os.getenv("EVENTS_API_HOST")
    configuration.verify_ssl = VERIFY_SSL

    try:
        events_api_client = EventsApi(ApiClient(configuration))
        events_publisher = EventsPublisher(
            events_api_client=events_api_client, publish_events=PUBLISH_EVENTS
        )
        monitor(events_publisher)
    except KeyboardInterrupt:
        logger.info("Polling stopped.")


def create_component_if_not_exists_and_set_schedule(
    component_key: str,
    component_name: str,
    component_role: str,
    component_tool: str,
    description: str,
    schedule_seconds: int,
    grace_period_seconds: int,
) -> ComponentHelper:
    """
    Checks if the components for the plugins are registered in the Events Ingestion API.
    If not, it registers them.
    """
    try:
        helper = ComponentHelper(
            key=f"{component_role} - {component_tool} - {component_key}",
            name=f"{component_role} - {component_tool} - {component_name}",
            tool=component_tool,
            description=description,
        )
        component_exists = helper.find_component()
        component_metadata = helper.create_component_if_not_exists()
        if not component_exists:
            helper.set_schedule(
                interval_minutes=int(schedule_seconds / 60),
                grace_period_seconds=grace_period_seconds,
            )
        logger.info(f"Found component {component_metadata}...")
        return helper
    except Exception:
        raise


def monitor(events_publisher: EventsPublisher) -> None:
    """
    Runs a loop to poll for new runs and updates to existing runs. On each polling
    interval, every plugin is called to fetch new runs that have started since the last interval.
    Subsequently, the update method is called on each run to discover any state changes and publish
    events accordingly.

    Parameters
    ----------
    events_publisher: EventPublisher
        Helper class instance for publishing events to the `Events Ingestions API
        <https://api.docs.datakitchen.io/production/events.html>`_.

    Returns
    -------
    None
    """
    logger.info(f"Plugins search paths: {PLUGINS_PATHS}")

    debug_first_pass = True

    # Make sure that the first run always updates the status
    last_heartbeat_update = datetime(1970, 8, 26, 0, 0, 0, 0, tzinfo=timezone.utc)

    # Start fetching runs from now onward. To fetch past runs, use time timedelta as shown below
    # execution_date_gte: datetime = (datetime.now() - timedelta(days=7)).astimezone()
    execution_date_gte = datetime.now(timezone.utc) - timedelta(
        seconds=int(os.getenv("DEBUG_FIRST_LOOKBACK", 0))
    )
    runs: List[AbstractRun] = []
    agents = {}
    while True:
        # Check if any new plugins have been added since the last poll interval.
        fetch_plugins(AbstractRunsFetcher, PLUGINS_PATHS)
        if len(AbstractRunsFetcher.plugins) == 0:
            raise Exception(
                "No plugins found. Please check your ENABLED_PLUGINS environment variable."
            )
        run_key = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M-%S-%f-%z")
        execution_date_lte: datetime = datetime.now().astimezone()
        for runs_fetcher in AbstractRunsFetcher.plugins:
            try:
                fetcher = runs_fetcher.create_runs_fetcher(events_publisher=events_publisher)
                if fetcher.agent_name not in agents:
                    heartbeat = create_component_if_not_exists_and_set_schedule(
                        fetcher.agent_key,
                        fetcher.agent_name,
                        agent_heartbeat_prefix,
                        fetcher.component_tool,
                        agent_heartbeat_description,
                        agent_heartbeat_schedule_seconds,
                        agent_heartbeat_grace_period_seconds,
                    )
                    freshness = create_component_if_not_exists_and_set_schedule(
                        fetcher.agent_key,
                        fetcher.agent_name,
                        agent_freshness_prefix,
                        fetcher.component_tool,
                        agent_freshness_description,
                        agent_freshness_schedule_seconds,
                        agent_freshness_grace_period_seconds,
                    )
                    agents[fetcher.agent_name] = {"heartbeat": heartbeat, "freshness": freshness}
                new_runs = fetcher.fetch_runs(execution_date_gte, execution_date_lte)
                unique_runs_found = 0
                for run in new_runs:
                    if run.run_key not in [r.run_key for r in runs]:
                        unique_runs_found += 1
                        runs.append(run)
                if unique_runs_found > 0:
                    events_publisher.publish_message_log_event(
                        log_level=MessageEventLogLevel.INFO,
                        event_timestamp=datetime.now(timezone.utc),
                        run_key=run_key,
                        task_key=None,
                        message=f"Found {unique_runs_found} new runs using {agents[fetcher.agent_name]['freshness'].name}",
                        pipeline_name=agents[fetcher.agent_name]["freshness"].name,
                        pipeline_key=agents[fetcher.agent_name]["freshness"].key,
                        component_tool=None,
                    )
            except Exception as e:
                logger.exception(e)
                events_publisher.publish_message_log_event(
                    log_level=MessageEventLogLevel.ERROR,
                    event_timestamp=datetime.now(timezone.utc),
                    run_key=run_key,
                    task_key=None,
                    message=f"{e}",
                    pipeline_name="POLLER-CATCHALL",
                    pipeline_key="POLLER-CATCHALL",
                    component_tool=None,
                )
                raise e
        execution_date_gte = execution_date_lte
        num_runs = len(runs)
        logger.info(f"Updating {num_runs} active runs...")
        start_time = time.time()
        num_finished_runs = 0

        # Send status update every 5 minutes
        if datetime.now(timezone.utc) - last_heartbeat_update > timedelta(
            seconds=heartbeat_interval_seconds
        ):
            # send status update
            for agent_name, agent_components in agents.items():
                events_publisher.publish_message_log_event(
                    log_level=MessageEventLogLevel.INFO,
                    event_timestamp=datetime.now(timezone.utc),
                    run_key=run_key,
                    task_key=None,
                    message=f"{agent_components['heartbeat'].name} is running.",
                    pipeline_name=agent_components["heartbeat"].name,
                    pipeline_key=agent_components["heartbeat"].key,
                    component_tool=None,
                )

            last_heartbeat_update = datetime.now(timezone.utc)

        # Process runs in parallel. This is an I/O bound process, so multi-threading is appropriate.
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_to_run = {executor.submit(lambda r: r.update(), r): r for r in runs}
            for future in as_completed(future_to_run):
                try:
                    run = future_to_run[future]
                    future.result()  # Exceptions won't be surfaced without this call.
                    if run.finished:
                        logger.info(f"Run {run.pipeline_key} ({run.run_key}) finished")
                        num_finished_runs += 1
                except Exception as e:
                    # TODO: If the run.update() command throws an exception every time it's called,
                    #  it will run forever. Need a way to flag runs that aren't updating and
                    #  handle them (e.g. log and remove).
                    logger.error(
                        f"Failed to update run {run.pipeline_key} ({run.run_key}): {traceback.format_exc()}"
                    )
                    raise e

        elapsed_time_secs = time.time() - start_time
        runs = [r for r in runs if not r.finished]
        logger.info(f"Finished updating {num_runs} runs in {elapsed_time_secs} seconds")
        logger.info(f"{num_finished_runs} runs finished and {len(runs)} remain active")
        time.sleep(POLLING_INTERVAL_SECS)


if __name__ == "__main__":
    main()
