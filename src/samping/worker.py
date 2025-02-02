import asyncio
import logging
import signal
from datetime import datetime, timezone

from .driver import QueueDriver
from .types import ASGI3Application
from .messages import Message
from .channel import channel, Sender
from .protocol import LifeSpanCyle, QueueCycle, beat_cycle
from .utils.timer import Timer
from .utils.time import get_exponential_backoff_interval
from .tasks import TaskStatus

logger = logging.getLogger("samping")

__all__ = [
    "Worker",
    "run_worker",
]


async def run_worker(app, queues: str, num_worker: int = 3, beat=False):
    worker = Worker(app, driver=app.driver)
    await worker.start(queues, num_worker=num_worker, beat=beat)


async def wrap_future(e, f):
    return e, await f


async def select(*futures):
    done, pendings = await asyncio.wait(
        [asyncio.ensure_future(wrap_future(label, fut)) for label, fut in futures],
        return_when=asyncio.FIRST_COMPLETED,
    )
    for task in pendings:
        if not task.cancelled():
            task.cancel()

    return done


class Worker:
    def __init__(self, app: ASGI3Application, driver: QueueDriver):
        self._app = app
        self._driver = driver
        self._stopping = asyncio.Event()

    def signal_handler(self, signame):
        logger.info("receive signal %s! exiting", signame)
        self._stopping.set()

    def install_signal_handler(self):
        loop = asyncio.get_running_loop()

        for signame in ("SIGINT", "SIGTERM"):
            loop.add_signal_handler(
                getattr(signal, signame),
                lambda: self.signal_handler(signame),
            )

    async def start(self, queues: str, num_worker: int = 3, beat=False):
        logger.info("starting %d workers processing queue %s", num_worker, queues)

        self.install_signal_handler()

        async with LifeSpanCyle(self._app) as lifespan:
            if lifespan.should_exit:
                return

            timer = Timer(60)
            if beat:
                timer.start()

            msg_tx, msg_rx = channel()
            producer = asyncio.create_task(self._run_producer(msg_tx, queues))
            workers = set()
            semaphore = asyncio.Semaphore(num_worker)
            ev_tx, ev_rx = channel()

            pending_tasks = 0
            while True:
                selected = await select(
                    ("message", msg_rx.recv()),
                    ("ending", self._stopping.wait()),
                    ("beat", timer.chan()),
                    ("task_event", ev_rx.recv()),
                )

                for task in selected:
                    which, result = task.result()
                    if which == "message":
                        logger.debug(
                            "receive message from broker, current pending task: %d",
                            pending_tasks,
                        )
                        task = asyncio.create_task(
                            self.on_message_received(semaphore, ev_tx, result)
                        )
                        workers.add(task)
                        task.add_done_callback(workers.discard)
                    elif which == "ending":
                        logger.info("warm shutdown..")
                        break
                    elif which == "beat":
                        logger.debug("receive beat event")
                        task = asyncio.create_task(
                            beat_cycle(self._app, datetime.now(timezone.utc))
                        )
                        workers.add(task)
                        task.add_done_callback(workers.discard)
                    elif which == "task_event":
                        logger.debug("task event received: %s", result)
                        if result == TaskStatus.PENDING:
                            pending_tasks += 1
                        elif result == TaskStatus.FINISHED:
                            pending_tasks -= 1
                else:
                    if producer.done():
                        logger.info("queue producer exited, starting producer")
                        producer = asyncio.create_task(self._run_producer(queues))
                    continue

                break

            if pending_tasks > 0:
                while pending_tasks > 0:
                    selected = await select(
                        ("ending", self._stopping.wait()), ("task_event", ev_rx.recv())
                    )
                    for task in selected:
                        which, result = task.result()
                        if which == "ending":
                            logger.info("Okay fine, shutting down now. See ya!")
                            break
                        elif which == "task_event":
                            if result == TaskStatus.PENDING:
                                pending_tasks += 1
                            elif result == TaskStatus.FINISHED:
                                pending_tasks -= 1
                    else:
                        continue

                    break

    async def on_message_received(
        self, semaphore: asyncio.Semaphore, tx: Sender[TaskStatus], message: Message
    ):
        async with semaphore:
            queue = QueueCycle(message, driver=self._driver, sender=tx)
            await queue.run_app(self._app)

    async def _run_producer(self, msg_tx: Sender[Message], queues: str):
        retries = 0
        while not self._stopping.is_set():
            try:
                async for message in self._driver.consume(queues):
                    await msg_tx.send(message)
            except BaseException:
                if not self._stopping.is_set():
                    retries += 1
                    wait = get_exponential_backoff_interval(2, retries, maximum=60)
                    logger.error(
                        "producer exited, waiting %d seconds to restart it",
                        wait,
                        exc_info=True,
                    )
                    await asyncio.sleep(wait)
                else:
                    break
