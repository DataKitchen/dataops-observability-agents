__all__ = ["NullSendChannel"]


from typing import Self

from trio._abc import SendChannel, SendType
from trio.lowlevel import checkpoint


class NullSendChannel(SendChannel):
    """
    Used to implement default arguments for functions which might have a SendChannel, or may not.

    Use this when your task does not connect to another task.
    """

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        await checkpoint()

    async def send(self, _: SendType) -> None:
        await checkpoint()

    def send_nowait(self, value: SendType) -> None:
        return

    async def aclose(self) -> None:
        await checkpoint()

    def clone(self) -> Self:
        return self
