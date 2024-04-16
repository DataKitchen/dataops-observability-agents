import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from typing import Any

from trio import current_time, sleep

from framework.core.tasks.periodic_task import PeriodicTask

from .loop import Loop

LOGGER = logging.getLogger(__name__)


async def _periodic_loop(period: float = 0.0) -> AsyncGenerator[float, None]:
    """
    An infinite loop helper. Note: Only period or deadline can be selected.

    Note: This implementation does not take the tasks' execution time into account. We can eventually
          pull in an implementation of this from Trio-Util.

    period:
        An absolute period; e.g., period == 5 means every run waits 5 seconds.
        0 means that the function just inserts a checkpoint
        (See: https://trio.readthedocs.io/en/stable/reference-core.html#checkpoints) without blocking.
    """
    while True:
        yield current_time()
        await sleep(period)


class PeriodicLoop(Loop[PeriodicTask]):
    """
    A periodic loop that executes a Task's action every period.
    """

    def __init__(self, period: float | int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.period = float(period)

    async def run(self) -> None:
        """
        Runs task.execute_task() with the current-time and previous-time as inputs. This input
        is timezone-aware.
        """
        current_dt = datetime.now(UTC)
        period_update = self.period
        keep_running = True
        async with self.task:
            while keep_running:
                async for _current_time in _periodic_loop(self.period):
                    prev_dt = current_dt
                    current_dt = datetime.now(UTC)
                    await self.task.execute_task(current_dt, prev_dt)
                    if self.task.is_done:
                        keep_running = False
                        break
                    if period_update := self.task.refresh_loop_period():
                        self.period = period_update
                        break
