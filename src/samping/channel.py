import asyncio
from typing import TypeVar, Generic, Tuple

T = TypeVar("T")


class Sender(Generic[T]):
    def __init__(self, queue: asyncio.Queue[T]):
        self._queue = queue

    async def send(self, value: T):
        await self._queue.put(value)


class Receiver(Generic[T]):
    def __init__(self, queue: asyncio.Queue[T]):
        self._queue = queue

    async def recv(self) -> T:
        value = await self._queue.get()
        self._queue.task_done()

        return value

    async def with_recv(self, action):
        value = await self._queue.get()
        await action(value)
        self._queue.task_done()


def channel(maxsize=0) -> Tuple[Sender[T], Receiver[T]]:
    queue = asyncio.Queue(maxsize=maxsize)

    return Sender(queue), Receiver(queue)
