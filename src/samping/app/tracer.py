import asyncio
import logging
import time

from ..tasks import Task, Request
from ..channel import Sender
from ..messages import Message
from ..exceptions import TaskExpectedError, TaskExpirationError, TaskTimeout, Retry

logger = logging.getLogger("samping")


class Tracer:
    def __init__(self, task: Task):
        self.task = task

    async def trace(self, monotonic=time.monotonic):
        """Trace the execution of task, return None if succeed, otherwise
        return datetime or seconds when this task should be retried
        """
        if self.is_expired:
            logger.warning(
                "Task %s expired, discarding %s", self.task.name, self.task.request.id
            )
            raise TaskExpirationError()

        start = monotonic()
        try:
            result = await self.execute_task()
        except (TaskTimeout, asyncio.exceptions.TimeoutError) as e:
            duration = monotonic() - start
            logger.warning(
                "Task %d timed out after %fs",
                self.task.name,
                duration,
            )
            return await self.handle_error(e, True)
        except Retry as e:
            logger.debug("Task %s triggered retry", self.task.name)
            return await self.handle_error(e, True, e.when)
        except TaskExpectedError as e:
            logger.warning(
                "Task %s failed with expected error: %s",
                self.task.name,
                self.task.request.id,
            )
            return await self.handle_error(e, True)
        except Exception as e:
            logger.warning(
                "Task %s failed with unexpected error: %s",
                self.task.name,
                self.task.request.id,
                exc_info=True,
            )
            return await self.handle_error(e, self.task.retry_for_error(e))
        else:
            # task executed successfully
            duration = monotonic() - start
            logger.info(
                "Task %s[%s] succeed in %0.4f seconds",
                self.task.name,
                self.task.request.id,
                duration,
            )

            await self.task.on_success(result)

            # TODO: run task callbacks

    async def handle_error(self, error, should_retry, retry_eta=None):
        await self.task.on_failure(
            error,
            self.task.request.id,
            self.task.request.args,
            self.task.request.kwargs,
        )

        # TODO: run task errcallbacks

        if not should_retry:
            return

        retries = self.task.request.retries
        if self.task.max_retries and retries >= self.task.max_retries:
            logger.warning(
                "Task %s[%s] retries exceeded", self.task.name, self.task.request.id
            )
            return

        return retry_eta or self.task.retry_delay()

    async def execute_task(self):
        if self.task.request.time_limit is None:
            return await self.task.run(
                *self.task.request.args, **self.task.request.kwargs
            )

        logger.debug(
            "Executing task %s with %0.4f seconds time limit",
            self.task.name,
            self.task.request.time_limit,
        )

        return await asyncio.wait_for(
            self.task.run(*self.task.request.args, **self.task.request.kwargs),
            timeout=self.task.request.time_limit,
        )

    @property
    def is_expired(self):
        return self.task.request.is_expired()

    @property
    def is_delayed(self):
        return self.task.request.is_delayed

    @property
    def acks_late(self):
        return self.task.acks_late

    async def wait(self):
        secs = self.task.request.countdown()
        if secs:
            await asyncio.sleep(secs)

    def as_retry_message(self, eta=None):
        return self.task.as_message_v2(
            task_id=self.task.request.id,
            name=self.task.name,
            args=self.task.request.args,
            kwargs=self.task.request.kwargs,
            eta=eta or self.task.request.eta,
            expires=self.task.request.expires,
            retries=self.task.request.retries + 1,
            callbacks=self.task.request.callbacks,
            errbacks=self.task.request.errbacks,
            chord=self.task.request.chord,
            chain=self.task.request.chain,
            group=self.task.request.group,
            time_limit=self.task.request.time_limit,
            origin=self.task.request.origin,
            reply_to=self.task.request.reply_to,
        )


def build_tracer(app, task: Task, message: Message, hostname):
    request = Request.from_message(app, message)
    request.hostname = hostname

    exec_task = task.from_request(request)

    return Tracer(exec_task)
