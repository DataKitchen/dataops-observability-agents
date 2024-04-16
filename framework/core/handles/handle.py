from abc import abstractmethod
from typing import Protocol, TypeVar

T = TypeVar("T")
R_co = TypeVar("R_co", covariant=True)


class Handle(Protocol[T, R_co]):
    """Implements the Handle protocol"""

    async def pre_hook(self) -> None:
        pass

    async def post_hook(self, value: T) -> R_co | T:
        return value

    @abstractmethod
    async def handle(self) -> T:
        raise NotImplementedError()
