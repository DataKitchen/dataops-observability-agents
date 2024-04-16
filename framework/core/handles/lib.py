import logging
from datetime import UTC, datetime

from framework.configuration import HTTPClientConfig
from registry import ConfigurationRegistry

LOG = logging.getLogger(__name__)


def has_retry_text(value: str | None) -> bool:
    """
    Check for the presence of known response text that correlates with a rate limit error.

    For rate limit errors that occur during an authentication attempt, rate-limit headers aren't always present.
    This check captures a case known to Auth0 and Github Enterprise.
    """
    if value:
        try:
            return "please try again in a bit" in value
        except (TypeError, ValueError):
            return False
    else:
        return False


def parse_rate_limit(value: float) -> float:
    """Parse a rate limit value if it is a timestamp. Never longer than default timeout."""
    # Since this value can either be a duration until reset or a timestamp representing the reset, if the value is
    # large (more than a day's worth of seconds) then it's probably a timestamp.
    if value > 86400:
        limit = value - datetime.now(UTC).timestamp()
    else:
        limit = value

    read_timeout = ConfigurationRegistry().lookup("http", HTTPClientConfig).read_timeout
    if limit > read_timeout:
        LOG.warning(
            "Rate limit value %s is longer than the requests timeout setting %s; defaulting to %s instead",
            limit,
            read_timeout,
            read_timeout,
        )
        return float(read_timeout)
    else:
        return float(limit)
