__all__ = ["Task"]

import logging
from abc import ABC, abstractmethod
from types import TracebackType
from typing import Any, Generic, Self

import trio
from trio import MemorySendChannel

from framework.configuration import T_CONFIG
from framework.core.channels import NullSendChannel
from toolkit.exceptions import UnrecoverableError
from toolkit.more_typing import JSON_DICT

LOGGER = logging.getLogger(__name__)


class Task(ABC, Generic[T_CONFIG]):
    """
    The base Task class.

    A task orchestrates the execution of Handles.

    """

    outbound_channel: MemorySendChannel[JSON_DICT] | NullSendChannel
    """
    An "outbound" channel, connecting the output of this task to another.
    if No task-connection is desired, pass a NullSendChannel instead.
    """

    def __init__(self, outbound_channel: MemorySendChannel[JSON_DICT] | NullSendChannel | None = None) -> None:
        if outbound_channel is None:
            self.outbound_channel = NullSendChannel()
        else:
            self.outbound_channel = outbound_channel
        self._channel_open = False

    async def __aenter__(self) -> Self:
        """
        See: https://docs.python.org/3/reference/datamodel.html#object.__aenter__
        The context-manager entry point. Implement any functionality here that requires collection on task-end.
        """
        await self.outbound_channel.__aenter__()
        self._channel_open = True
        return self

    async def __aexit__(self, exc_type: type[BaseException], exc_val: BaseException, exc_tb: TracebackType) -> None:
        """
        See: https://docs.python.org/3/reference/datamodel.html#object.__aexit__.
        Context-manager's exit point. Implement any resource collection that needs to happen at the end of the
        task's lifetime.
        """
        await self.outbound_channel.__aexit__(exc_type, exc_val, exc_tb)
        self._channel_open = False

    async def send(self, payload: JSON_DICT) -> None:
        """
        Sends the given payload to the bound MemorySendChannel.

        Note: If your code does not send to another task, set self.outbound_channel to an instance of NullSendChannel.
        """
        if not self._channel_open:
            raise RuntimeError("The channel was not opened; Did you run task as a context?")
        await self.outbound_channel.send(payload)

    async def execute_task(self, *args: Any, **kwargs: Any) -> None:
        """
        execute_task() that wraps execute(). Other classes should call
        execute_task() rather than execute() directly.

        Override this function only if you need to handle how execute is called
        or how its cancellation is handled.
        """
        # cancel-caught set just to shut up type-checker.
        with trio.CancelScope() as cancel:
            try:
                await self.execute(*args, **kwargs)
            # Per AG-122, propagate exception to main and exit on unauthorized OBS requests (invalid SA Key)
            except UnrecoverableError:
                cancel.cancel()
                raise
            except:
                cancel.cancel()
                LOGGER.exception("Uncaught error during task execution")

    @abstractmethod
    async def execute(self, *args: Any, **kwargs: Any) -> None:
        """
        execute the main body of the task. Overriding this function is mandatory.
        """
        raise NotImplementedError()
