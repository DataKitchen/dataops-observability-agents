import logging
import sys

import trio
from pydantic import ValidationError

from agents.airflow.agent import main as airflow_agent
from agents.databricks.agent import main as databricks_agent
from agents.eventhub.agent import main as eventhub_agent
from agents.powerbi.agent import main as powerbi_agent
from agents.qlik.agent import main as qlik_agent
from agents.ssis.main import main as ssis_agent
from agents.synapse_analytics.agent import main as synapse_analytics_main
from agents.target_example.agent import main as example_agent
from framework.configuration import CoreConfiguration
from registry import ConfigurationRegistry
from toolkit.exceptions import UnrecoverableError
from toolkit.logging_tools import parse_validation_error
from toolkit.logging_tools.configure_logging import logging_init


async def main() -> None:
    core_config = ConfigurationRegistry().lookup("core", CoreConfiguration)
    logging_init(
        level=core_config.log_level.upper(),
        agent_level=core_config.log_level.upper(),
        library_level=core_config.log_level.upper(),
    )
    logger = logging.getLogger("framework.__main__.main")
    try:
        match core_config.agent_type:
            case "eventhubs":
                await eventhub_agent()
            case "databricks":
                await databricks_agent()
            case "ssis":
                await ssis_agent()
            case "synapse_analytics":
                await synapse_analytics_main()
            case "airflow":
                await airflow_agent()
            case "example_agent":
                await example_agent()
            case "power_bi":
                await powerbi_agent()
            case "qlik":
                await qlik_agent()
            case _:
                logger.error("Error starting up agent, unknown agent_type '%s' configured", core_config.agent_type)
                raise RuntimeError(f"Nonexistent Agent: {core_config.agent_type}")  # noqa: TRY301
    # Per AG-122, shut down on unauthorized OBS requests (invalid SA Key)
    except UnrecoverableError:
        sys.exit(1)
    except ValidationError as e:
        logger.exception(parse_validation_error(e))  # noqa: TRY401
        sys.exit(1)
    except Exception:
        logger.exception("Error starting up agent.")
        sys.exit(1)


def cli() -> None:
    trio.run(main)


if __name__ == "__main__":
    cli()
