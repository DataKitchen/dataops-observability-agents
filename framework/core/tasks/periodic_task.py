__all__ = ["PeriodicTask"]

import logging
from abc import abstractmethod
from datetime import datetime
from typing import Any

from framework.core.tasks.task import Task

LOGGER = logging.getLogger(__name__)


class PeriodicTask(Task):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._is_done = False
        self._update_period = 0.0

    @abstractmethod
    async def execute(self, current_dt: datetime, previous_dt: datetime) -> None:
        raise NotImplementedError()

    @property
    def is_done(self) -> bool:
        return self._is_done

    def finish(self) -> None:
        self._is_done = True

    def update_loop_period(self, new_period: float) -> None:
        self._update_period = new_period

    def refresh_loop_period(self) -> float:
        new_period = self._update_period
        self._update_period = 0
        return new_period
