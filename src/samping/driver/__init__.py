from typing import List, Protocol, AsyncGenerator
from abc import abstractmethod
from collections import OrderedDict
import asyncio

from ..messages import Message


class QueueDriver(Protocol):
    @abstractmethod
    async def send_batch(self, queue: str, messages: List[Message], **kwargs) -> None:
        ...

    @abstractmethod
    async def consume(self, queues: str) -> AsyncGenerator[Message, None]:
        ...

    @abstractmethod
    async def send_ack(self, message: Message) -> None:
        ...

    @abstractmethod
    async def send_nack(self, message: Message, delay: int = 1) -> None:
        ...


class MockDriver(Protocol):
    def __init__(self):
        self._lock = asyncio.Lock()
        self.queues = OrderedDict()

    async def send_batch(self, queue, message: List[Message], **kwargs):
        async with self._lock:
            if queue not in self.queues:
                self.queues[queue] = message
            else:
                messages = self.queues[queue]
                messages.extend(queue)

    async def send_concume(self, queues):
        raise NotImplemented("not implemented")

    async def send_ack(self, message):
        pass

    async def send_nack(self, message, delay):
        pass
