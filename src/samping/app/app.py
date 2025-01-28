import asyncio
import logging
from datetime import datetime, timezone
from croniter import croniter
from functools import cached_property
import traceback
import uuid
import numbers
from typing import Optional, List, Callable, Union, Dict

from ..driver import QueueDriver
from ..tasks import Task, CronJob
from ..messages import Message
from ..exceptions import DecodeError, ContentDisallowed
from ..routes import route, Rule
from ..utils.format import utcnow, parse_iso8601
from ..utils.time import timezone as utils_timezone, to_utc
from ..types import Lifespan, AppType, Scope, Receive, Send
from .conf import Conf
from .tracer import build_tracer


async def run_tab(logger, tab, name):
    logger.info("executing cron tab: %s", name)
    await tab.run()


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


default_conf = {
    "timezone": None,
    "task_serializer": "msgpack",
}


class _DefaultLifespan:
    async def __aenter__(self):
        pass

    async def __aexit__(self, *exc_info: object) -> None:
        pass

    def __call__(self, app) -> None:
        return self


class App:
    def __init__(
        self,
        driver_factory: Callable[[], QueueDriver],
        default_queue: str = "default",
        routes: Optional[List[Rule]] = None,
        lifespan: Lifespan[AppType] | None = None,
    ):
        self.driver_factory = driver_factory
        self._driver: Union[None, QueueDriver] = None
        self.logger = logging.getLogger("samping")
        self._tasks: Dict[str, Task] = {}
        self._cron_tabs: Dict[str, CronJob] = {}
        self.default_queue = default_queue
        self.routes = routes or None
        self.lifespan = lifespan if lifespan else _DefaultLifespan()
        self._conf = Conf(default_conf)

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

    def now(self):
        now_in_utc = to_utc(utcnow())
        return now_in_utc.astimezone(self.timezone)

    def tab(self, **opts):
        def create_job_cls(fun):
            return self._cron_from_fun(fun, **opts)

        return create_job_cls

    def task(self, **opts):
        def create_task_cls(fun):
            return self._task_from_fun(fun, **opts)

        return create_task_cls

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        assert scope["type"] in ("lifespan", "beat", "queue")

        if scope["type"] == "lifespan":
            return await self.lifespan(scope, receive, send)
        if scope["type"] == "beat":
            return await self.beat(scope)

        headers = scope["headers"]
        task_name = headers.get("task", None)
        if task_name:
            return await self.handle_v2_message(scope, receive, send)
        else:
            return await self.handle_v1_message(scope, receive, send)

    async def beat(self, scope: Scope):
        try:
            event_time = parse_iso8601(scope.get("time", ""))
            event_time = event_time.astimezone(tz=self.timezone)
        except ValueError:
            event_time = self.now()

        await self.run_matched_tabs(current_date=event_time)

    async def handle_v1_message(self, scope: Scope, receive: Receive, send: Send):
        headers = scope["headers"]
        properties = scope.get("properties", {})
        message = Message(
            scope["body"],
            headers=headers,
            properties=properties,
            content_type=scope["content_type"],
            content_encoding=scope["content_encoding"],
        )
        try:
            task_message = message.decode()
        except (DecodeError, ContentDisallowed):
            self.logger.warning("Received invalid message, discarding")
            return await send({"type": "queue.ack"})
        else:
            task_name = task_message["task"]
            if task_name not in self._tasks:
                self.logger.warning(
                    "Receive message for task %s, but it is not in registered task, discarding.",
                    task_name,
                )
                await send({"type": "queue.ack"})
                return

            task = self._tasks[task_name]
            tracer = build_tracer(self, task, message, scope.get("hostname", ""))
            if tracer.is_delayed:
                await tracer.wait()

            if not tracer.acks_late:
                await send({"type": "queue.ack"})

            retry = await tracer.trace()
            if retry:
                if isinstance(retry, numbers.Real):
                    await send({"type": "queue.nack", "delay": retry})
                else:
                    delay = (self.now() - retry).total_seconds()
                    await send({"type": "queue.nack", "delay": delay})
            else:
                if tracer.acks_late:
                    await send({"type": "queue.ack"})

    async def handle_v2_message(self, scope: Scope, receive: Receive, send: Send):
        headers = scope["headers"]
        properties = scope.get("properties", {})
        task_name = headers.get("task", None)

        if task_name not in self._tasks:
            self.logger.warning(
                "Receive message for task %s, but it is not in registered task, discarding.",
                task_name,
            )
            await send({"type": "queue.ack"})
            return

        task = self._tasks[task_name]
        message = Message(
            scope["body"],
            headers=headers,
            properties=properties,
            content_type=scope["content_type"],
            content_encoding=scope["content_encoding"],
        )
        try:
            tracer = build_tracer(self, task, message, scope.get("hostname", ""))
        except (DecodeError, ContentDisallowed):
            self.logger.warning("Received invalid message, discarding")
            return await send({"type": "queue.ack"})

        if tracer.is_delayed:
            await tracer.wait()

        if not tracer.acks_late:
            await send({"type": "queue.ack"})

        retry = await tracer.trace()
        if retry:
            if isinstance(retry, numbers.Real):
                await send({"type": "queue.nack", "delay": retry})
            else:
                delay = (self.now() - retry).total_seconds()
                await send({"type": "queue.nack", "delay": delay})
        else:
            if tracer.acks_late:
                await send({"type": "queue.ack"})

    async def lifespan(self, scope: Scope, receive: Receive, send: Send):
        started = False
        await receive()
        try:
            async with self.lifespan_context(self) as maybe_state:
                if maybe_state is not None:
                    if "state" not in scope:
                        raise RuntimeError(
                            "The server/worker does not support state in the lifespan scope"
                        )
                    scope["state"].update(maybe_state)
                await send({"type": "lifespan.startup.completed"})
                started = True
                await receive()
        except BaseException:
            exc_text = traceback.format_exc()
            if started:
                await send({"type": "lifespan.shutdown.failed", "message": exc_text})
            else:
                await send({"type": "lifespan.started.failed", "message": exc_text})
        else:
            await send({"type": "lifespan.shutdown.complete"})

    async def run_matched_tabs(self, current_date: Optional[datetime] = None):
        n = self.now() if current_date is None else current_date
        tasks = []
        for name, tab in self._cron_tabs.items():
            if croniter.match(tab.expression, n):
                task = asyncio.create_task(
                    run_tab(self.logger, tab, name),
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

    @property
    def conf(self):
        """Current configuration"""
        return self._conf

    @cached_property
    def timezone(self):
        conf = self.conf
        if not conf.timezone:
            return timezone.utc

        return utils_timezone.get_timezone(conf.timezone)
