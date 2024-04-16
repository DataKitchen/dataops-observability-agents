import logging
from collections.abc import Callable
from http import HTTPStatus
from typing import Any

from httpx import HTTPStatusError

from toolkit.exceptions import UnrecoverableError

LOGGER = logging.getLogger(__name__)


def handle_observability_exception(f: Callable) -> Callable:
    async def wrapper(*args: Any, **kwargs: Any) -> None:
        try:
            await f(*args, **kwargs)
        except HTTPStatusError as e:
            if e.response.status_code == HTTPStatus.UNAUTHORIZED:
                # don't need to expose stack traces to the users as this is a foreseeable type of error
                LOGGER.error(  # noqa: TRY400
                    "Unable to authorize with DataKitchen Observability, invalid Service Account key. "
                    "Please verify if the key has expired or reach out for IT support.",
                )
                raise UnrecoverableError from e
            else:
                raise

    return wrapper
