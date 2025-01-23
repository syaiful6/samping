import asyncio
import logging
import base64
from datetime import datetime
from croniter import croniter
import signal
import uuid
from typing import Optional, List, Callable, Union, Dict, NamedTuple

from .driver import QueueDriver
from .exceptions import Retry
from .tasks import Task, CronJob
from .messages import Message
from .routes import route, Rule
from .backoff import Exponential
from .worker import Worker
from .utils import try_to_int, utcnow, parse_iso8601


async def run_tab(logger, tab, name):
    logger.info("executing cron tab: %s", name)
    await tab.run()


class WorkerData(NamedTuple):
    name: str
    task: asyncio.Task


class App:
    def __init__(
        self,
        driver_factory: Callable[[], QueueDriver],
        task_timeout: int = 300,
        queue_size: int = 100,
        default_queue: str = "default",
        routes: Optional[List[Rule]] = None,
        disable_cron: bool = False,
        worker_max_tasks: Optional[int] = None,
    ):
        self._task_timeout = task_timeout
        self._queue_size = queue_size
        self.driver_factory = driver_factory
        self._driver: Union[None, QueueDriver] = None
        self.logger = logging.getLogger("samping")
        self._tasks: Dict[str, Task] = {}
        self._cron_tabs: Dict[str, CronJob] = {}
        self._loop = None
        self._queue = None
        self.default_queue = default_queue
        self.routes = routes or None
        self.disable_cron = disable_cron
        self.worker_dict: Dict[str, WorkerData] = {}
        self._num_worker = 0
        self._stopping = False
        self._worker_max_tasks = worker_max_tasks

    def task_route(self, task: Task) -> str:
        queue = route(task.name, self.routes)
        self.logger.debug(
            "route %s to queue %s", task.name, queue or self.default_queue
        )
        return queue or self.default_queue

    @property
    def driver(self):
        if self._driver is None:
            self._driver = self.driver_factory()

        return self._driver

    @property
    def active_workers(self):
        return len(self.worker_dict)

    @property
    def queue(self):
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self._queue_size)
        return self._queue

    @property
    def loop(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
        return self._loop

    def tab(self, **opts):
        def create_job_cls(fun):
            return self._cron_from_fun(fun, **opts)

        return create_job_cls

    def task(self, **opts):
        def create_task_cls(fun):
            return self._task_from_fun(fun, **opts)

        return create_task_cls

    async def run_matched_tabs(self, current_date: Optional[datetime] = None):
        n = utcnow() if current_date is None else current_date
        tasks = []
        for name, tab in self._cron_tabs.items():
            if croniter.match(tab.expression, n):
                task = asyncio.create_task(
                    asyncio.wait_for(
                        run_tab(self.logger, tab, name), timeout=self._task_timeout
                    ),
                    name=name + "-" + str(uuid.uuid4()),
                )
                tasks.append(task)
        if tasks:
            try:
                done, _ = await asyncio.wait(tasks)
                for task in done:
                    try:
                        await task
                    except Exception as e:
                        self.logger.error(
                            f"cron job {task.get_name()} exited with error: {e}"
                        )
            except Exception as e:
                self.logger.error(f"cron jobs exited with error: {e}")

    def __call__(self, event: dict, context: dict):
        if self.is_queue_event(event):
            messages = self.decode_sqs_events(event["Records"])
            self.logger.debug("receive sqs queue %d", len(messages))
            self.on_receive_message(messages)
        elif self.is_scheduled_event(event):
            self.handle_scheduled_event(event)
        else:
            raise TypeError("Invalid SQS event or EventBridge event")

    def is_scheduled_event(self, event: dict) -> bool:
        return event.get("source", "") == "aws.events" and "time" in event

    def is_queue_event(self, event: dict) -> bool:
        return (
            "Records" in event
            and len(event["Records"]) > 0
            and "body" in event["Records"][0]
        )

    async def _signal_handler(self, signame):
        self.logger.info("receive signal %s! exiting", signame)
        self._stopping = True
        await self.queue.join()
        self.loop.stop()

    async def run_worker(self, queues: str, num_worker: int = 3):
        self.logger.info("starting %d workers processing queue %s", num_worker, queues)
        for signame in ("SIGINT", "SIGTERM"):
            self.loop.add_signal_handler(
                getattr(signal, signame),
                lambda: asyncio.create_task(self._signal_handler(signame)),
            )

        self._num_worker = num_worker
        producer = asyncio.create_task(self._run_producer(queues))
        cron_task = None
        if not self.disable_cron:
            cron_task = asyncio.create_task(self._run_cron())

        self._run_worker(num_worker)

        while True:
            if self._stopping:
                break
            if producer.done():
                self.logger.info("queue producer exited, starting producer")
                producer = asyncio.create_task(self._run_producer(queues))

            if cron_task and cron_task.done():
                self.logger.info("cron worker exited, starting new process")
                cron_task = asyncio.create_task(self._run_cron())

            self.logger.debug("active workers: %d", self.active_workers)

            self.reap_workers()

            await asyncio.sleep(10)

    async def _run_producer(self, queues: str):
        try:
            async for message in self.driver.consume(queues):
                self.logger.debug("got a new task message, put it in internal queue")
                await self.queue.put(message)
        except Exception:
            self.logger.info("producer exited...")

    def _run_worker(self, num_worker: int = 3):
        for _ in range(num_worker):
            name = uuid.uuid4().hex
            task = asyncio.create_task(self.worker(name), name=name)
            self.worker_dict[name] = WorkerData(name=name, task=task)
            task.add_done_callback(self.restart_worker)

    async def worker(self, name):
        worker = Worker(
            self._tasks,
            self.driver,
            self.default_queue,
            name=name,
            task_timeout=self._task_timeout,
        )
        await worker.work(self.queue, max_tasks=self._worker_max_tasks)

    def restart_worker(self, task: asyncio.Task):
        name = task.get_name()
        if name in self.worker_dict:
            del self.worker_dict[name]
        self.logger.info("Worker %s exiting: spawning new asyncio task", name)
        self._run_worker(num_worker=1)

    def reap_workers(self):
        self.logger.debug("reaping dead workers")
        worker_datas = list(self.worker_dict.values())
        for data in worker_datas:
            if data.task.done():
                self.restart_worker(data.task)

    async def _run_cron(self):
        while True:
            await self.run_matched_tabs()
            await asyncio.sleep(60)

    def decode_sqs_events(self, sqs_messages: List[dict]) -> List[Message]:
        messages: List[Message] = []
        for sqs_message in sqs_messages:
            body = base64.standard_b64decode(sqs_message.get("body", ""))
            attributes = sqs_message.get("attributes", {})
            receiptHandle = sqs_message.get("receiptHandle", "")
            queue = sqs_message.get("eventSourceARN", "").split(":")[-1]
            messages.append(
                Message(
                    receiptHandle,
                    body,
                    queue=queue,
                    receive_count=try_to_int(
                        attributes.get("ApproximateReceiveCount", 0)
                    ),
                )
            )
        return messages

    def on_receive_message(self, messages: List[Message]):
        handle_message_instance = self.handle_messages(messages, managed=False)
        handle_message_task = self.loop.create_task(handle_message_instance)
        self.loop.run_until_complete(handle_message_task)

    async def handle_messages(self, messages: List[Message], managed: bool = True):
        await asyncio.gather(
            *[self.handle_message(message, managed=managed) for message in messages]
        )

    async def handle_message(self, message: Message, managed: bool = True):
        task_message = message.decode_task_message()
        queue_name = message.queue or self.default_queue
        if task_message.expires is not None and task_message.expires < utcnow():
            await self.driver.send_ack(message.ack_id, queue_name)
        if task_message.task not in self._tasks:
            await self.driver.send_ack(message.ack_id, queue_name)
            return
        task = self._tasks[task_message.task]
        try:
            self.logger.debug("handle message %s", task.name)
            await asyncio.wait_for(
                task.run(*task_message.args, **task_message.kwargs),
                timeout=self._task_timeout,
            )
            await self.driver.send_ack(message.ack_id, queue_name)
        except Retry as exc:
            if managed:
                if task_message.retries < message.receive_count:
                    await self.driver.send_ack(message.ack_id, queue_name)
                else:
                    await self.driver.send_nack(
                        message.ack_id, queue_name, delay=exc.when or 1
                    )
            else:
                raise exc
        except Exception as exc:
            self.logger.exception("handle task %s returned an error", task_message.task)
            if managed:
                if task_message.retries < message.receive_count:
                    await self.driver.send_ack(message.ack_id, queue_name)
                else:
                    await self.driver.send_nack(message.ack_id, queue_name)
            else:
                raise exc

    async def _send_message(
        self, message: Message, queue: Optional[str] = None, **kwargs
    ):
        return await self.send_batch(
            messages=[message], queue=queue or self.default_queue, **kwargs
        )

    async def send_batch(
        self, messages: List[Message], queue: Optional[str] = None, **kwargs
    ):
        return await self.driver.send_batch(
            queue=queue or self.default_queue, messages=messages, **kwargs
        )

    def handle_scheduled_event(self, event: dict):
        if self.is_scheduled_event(event):
            try:
                event_time = parse_iso8601(event.get("time", ""))
            except ValueError:
                event_time = None

            cron_instance = self.run_matched_tabs(current_date=event_time)
            cron_task = self.loop.create_task(cron_instance)
            self.loop.run_until_complete(cron_task)
        else:
            raise TypeError("not an event bridge event")

    def _task_from_fun(self, fun, base=None, name=None, bind=False, **options) -> Task:
        name = name or self.gen_task_name(fun.__name__, fun.__module__)
        base = base or Task
        if name not in self._tasks:
            run = fun if bind else staticmethod(fun)
            task = type(
                fun.__name__,
                (base,),
                dict(
                    {
                        "app": self,
                        "name": name,
                        "run": run,
                        "_decorated": True,
                        "__doc__": fun.__doc__,
                        "__module__": fun.__module__,
                        "__annotations__": fun.__annotations__,
                        "__wrapped__": fun,
                    },
                    **options,
                ),
            )()
            try:
                task.__qualname__ = fun.__qualname__
            except AttributeError:
                pass

            self._tasks[task.name] = task
            task.bind(self)
        else:
            task = self._tasks[name]

        return task

    def _cron_from_fun(
        self, fun, base=None, name=None, bind=False, **options
    ) -> CronJob:
        name = name or self.gen_task_name(fun.__name__, fun.__module__)
        base = base or CronJob
        if name not in self._cron_tabs:
            run = fun if bind else staticmethod(fun)
            task = type(
                fun.__name__,
                (base,),
                dict(
                    {
                        "app": self,
                        "name": name,
                        "run": run,
                        "_decorated": True,
                        "__doc__": fun.__doc__,
                        "__module__": fun.__module__,
                        "__annotations__": fun.__annotations__,
                        "__wrapped__": fun,
                    },
                    **options,
                ),
            )()
            try:
                task.__qualname__ = fun.__qualname__
            except AttributeError:
                pass
            self._cron_tabs[task.name] = task
        else:
            task = self._cron_tabs[name]
        return task

    def gen_task_name(self, name: str, module_name: str) -> str:
        return ".".join(p for p in (module_name, name) if p)
