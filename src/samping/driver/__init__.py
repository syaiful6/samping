from typing import List, Protocol, AsyncGenerator
from abc import abstractmethod

from ..messages import Message


class QueueDriver(Protocol):
    @abstractmethod
    async def send_batch(self, queue: str, messages: List[Message], **kwargs) -> None:
        ...

    @abstractmethod
    async def consume(self, queues: str) -> AsyncGenerator[Message, None]:
        ...

    @abstractmethod
    async def send_ack(self, ack_id: str, queue: str = None) -> None:
        ...

    @abstractmethod
    async def send_nack(self, ack_id: str, queue: str = None, delay: int = 1) -> None:
        ...
