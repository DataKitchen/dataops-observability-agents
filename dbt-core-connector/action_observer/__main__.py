import json
import logging
import os

from events_ingestion_client import ApiClient, Configuration, EventsApi

from action_observer.commands.dbt_core import DBTResultsPublisher
from action_observer.common import events
from action_observer.common.events.events_publisher import EventsPublisher

PUBLISH_EVENTS = os.getenv("PUBLISH_EVENTS", "true").lower() in ["true", "1"]


def configure_logging(level: int = logging.DEBUG) -> None:
    logger = logging.getLogger()
    handler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s %(name)-12s %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)


class MissingEnvVariableError(Exception):
    pass


def check_required_env_variables(required_vars: list) -> list[str]:
    missing_vars = []
    for var in required_vars:
        if var not in os.environ:
            missing_vars.append(var)
    return missing_vars


def cli() -> None:
    configure_logging()

    missing_variables = check_required_env_variables(
        ["RUN_RESULTS_PATH", "MANIFEST_PATH", "PIPELINE_KEY", "PIPELINE_NAME", "EVENTS_API_KEY"]
    )
    if missing_variables:
        raise MissingEnvVariableError(
            f"The following required environment variables are missing: {', '.join(missing_variables)}"
        )

    # Configure API key authorization: SAKey
    configuration = Configuration()
    configuration.api_key["ServiceAccountAuthenticationKey"] = os.getenv("EVENTS_API_KEY")
    configuration.host = os.getenv("EVENTS_API_HOST", "https://api.datakitchen.io")

    try:
        events_api_client = EventsApi(ApiClient(configuration))
        events_publisher = EventsPublisher(events_api_client=events_api_client, publish_events=PUBLISH_EVENTS)
    except KeyboardInterrupt:
        logging.info("Polling stopped.")
        raise

    run_results_path = os.getenv("RUN_RESULTS_PATH")
    if run_results_path is not None:
        with open(run_results_path) as run_results_file:
            run_results = json.load(run_results_file)
    else:
        raise MissingEnvVariableError("The following required environment variables are missing: RUN_RESULTS_PATH")

    manifest_path = os.getenv("MANIFEST_PATH")
    if manifest_path is not None:
        with open(manifest_path) as manifest_file:
            manifest = json.load(manifest_file)
    else:
        raise MissingEnvVariableError("The following required environment variables are missing: MANIFEST_PATH")

    pipeline_key = os.getenv("PIPELINE_KEY")
    if pipeline_key is None:
        raise MissingEnvVariableError("The following required environment variables are missing: PIPELINE_KEY")
    pipeline_name = os.getenv("PIPELINE_NAME")
    if pipeline_name is None:
        raise MissingEnvVariableError("The following required environment variables are missing: PIPELINE_NAME")
    dbt_publisher = DBTResultsPublisher(
        manifest=manifest,
        run_results=run_results,
        events_publisher=events_publisher,
        pipeline_key=pipeline_key,
        pipeline_name=pipeline_name,
    )
    dbt_publisher.parse()

    logging.info(events.Status.COMPLETED)


if __name__ == "__main__":
    cli()
