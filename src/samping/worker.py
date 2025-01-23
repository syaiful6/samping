from enum import StrEnum
from typing import Dict, Optional
import asyncio
import logging
import uuid

from .driver import QueueDriver
from .exceptions import Retry
from .tasks import Task, TaskMessage
from .messages import Message
from .utils import utcnow


logger = logging.getLogger("samping")


class WorkerStatus(StrEnum):
    STARTED = "started"
    SUSPENDED = "suspended"
    STOPPING = "stopping"
    STOPPED = "stepped"
    BUSY = "busy"
    IDLE = "idle"


class Worker:
    def __init__(
        self,
        tasks: Dict[str, Task],
        driver: QueueDriver,
        default_queue: str,
        name: Optional[str] = None,
        task_timeout: int = 300,
    ):
        self.tasks = tasks
        self.driver = driver
        self.default_queue = default_queue
        self.task_timeout = task_timeout
        self._state = WorkerStatus.IDLE
        self.name = name

    def _get_state(self):
        return self._state

    def _set_state(self, state):
        self._state = state

    state = property(_get_state, _set_state)

    def stop(self):
        self.state = WorkerStatus.STOPPING
        logger.info("Worker %s: requested to stop")

    async def work(
        self,
        queue: asyncio.Queue[Message],
        max_tasks: Optional[int] = None,
    ):
        self.state = WorkerStatus.STARTED
        completed_tasks = 0
        while True:
            try:
                if self.state == WorkerStatus.STOPPING:
                    logger.info("Worker %s: stopping on request", self.name)
                    self._state = WorkerStatus.STOPPED
                    break

                message = await queue.get()

                try:
                    await self.handle_message(message)
                finally:
                    queue.task_done()

                completed_tasks += 1
                if max_tasks is not None and completed_tasks >= max_tasks:
                    logger.info(
                        "Worker %s: finished executing %d tasks, quitting",
                        self.name,
                        max_tasks,
                    )
                    break
            except:
                logger.error(
                    "Worker %s: found an unhandled exception, quitting...",
                    self.name,
                    exc_info=True,
                )
                break

        return completed_tasks

    async def handle_message(self, message: Message):
        task_message = message.decode_task_message()
        queue_name = message.queue or self.default_queue

        if task_message.expires is not None and task_message.expires < utcnow():
            await self.driver.send_ack(message.ack_id, queue_name)
            return

        if task_message.task not in self.tasks:
            logger.warning("Task %s not in registered tasks, aborting.", task_message.task)
            await self.driver.send_ack(message.ack_id, queue_name)
            return

        task = self.tasks[task_message.task]

        self.state = WorkerStatus.BUSY

        await self.execute_task(
            ack_id=message.ack_id,
            queue_name=queue_name,
            task=task,
            task_message=task_message,
            received_count=message.receive_count,
        )

        self.state = WorkerStatus.IDLE

    async def execute_task(
        self,
        ack_id: str,
        queue_name: str,
        task: Task,
        task_message: TaskMessage,
        received_count: int = 0,
    ):
        try:
            logger.debug("executing task %s in worker %s", task.name, self.name)
            await asyncio.wait_for(
                task.run(*task_message.args, **task_message.kwargs),
                timeout=self.task_timeout,
            )
            logger.debug(
                "task %s with id %s executed", task.name, task_message.id
            )
            await self.driver.send_ack(ack_id, queue_name)
        except Retry as exc:
            if task_message.retries < received_count:
                await self.driver.send_ack(ack_id, queue_name)
            else:
                await self.driver.send_nack(ack_id, queue_name, delay=exc.when or 1)
        except Exception as exc:
            logger.exception("execute task %s returned an error", task_message.task)
            if task_message.retries < received_count:
                await self.driver.send_ack(ack_id, queue_name)
            else:
                await self.driver.send_nack(ack_id, queue_name)

    async def teardown(self):
        pass
