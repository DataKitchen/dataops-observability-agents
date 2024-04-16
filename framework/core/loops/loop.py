from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from framework.core.tasks import Task

T_TASK = TypeVar("T_TASK", bound=Task)


class Loop(ABC, Generic[T_TASK]):
    """
    ABC for Loops.

    A loop tasks a task and executes it on some recurring input. For example, a timespan or an infinite source
    like a memory channel. For an example, see:
      *  framework/loops/memory_channel_loop.py
      *  framework/loops/periodic_loop.py
    """

    def __init__(self, task: T_TASK) -> None:
        self.task = task

    @abstractmethod
    async def run(self) -> None:
        """
        Function that will run the loop. This is the main implementation and is required.
        """
        raise NotImplementedError("Loop implementation undefined. Use one of the run implementations instead")
