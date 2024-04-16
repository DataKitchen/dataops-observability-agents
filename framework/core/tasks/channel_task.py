__all__ = ["ChannelTask"]

import logging
from abc import abstractmethod
from typing import Any, Generic

from toolkit.more_typing import T_RECEIVABLE

from .task import Task

LOGGER = logging.getLogger(__name__)


class ChannelTask(Task, Generic[T_RECEIVABLE]):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)

    @abstractmethod
    async def execute(self, receivable: T_RECEIVABLE) -> None:
        raise NotImplementedError()
