import asyncio


class Timer:
    def __init__(self, duration: int):
        self.duration = duration
        self._queue = asyncio.Queue()
        self._task = None
        self._stopped = False

    async def chan(self):
        await self._queue.get()

    def stop(self):
        self._stopped = True
        self._task = None

    def start(self):
        if self._task is not None:
            return
        loop = asyncio.get_running_loop()
        task = loop.create_task(self.run_loop())
        # keep reference to task
        self._task = task

    async def run_loop(self):
        while not self._stopped:
            await asyncio.sleep(self.duration)
            await self._queue.put(None)
