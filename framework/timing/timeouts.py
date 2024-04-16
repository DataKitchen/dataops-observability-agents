import logging
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from trio import CancelScope, TooSlowError, fail_after

LOGGER = logging.getLogger(__name__)


@asynccontextmanager
async def timeout_scope_log(timeout: float, name: str) -> AsyncGenerator[CancelScope, None]:
    try:
        c: CancelScope
        with fail_after(timeout) as c:  # noqa: TRIO100
            yield c
    except TooSlowError:
        logging.exception("Could not complete '%s'. Failed after %f seconds.", name, timeout)
        raise
    except Exception:
        raise
