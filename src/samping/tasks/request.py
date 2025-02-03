import typing
from datetime import datetime
from dataclasses import dataclass
from ..messages import Message, MessageBodyV1
from ..utils.format import try_to_int
from ..utils.time import maybe_iso8601


@dataclass
class Request:
    # any for now, it reference to samping app
    app: typing.Any

    # the unique id of the executing task
    id: str

    # Custom ID used for things like de-duplication. Usually the same as `id`.
    correlation_id: str

    # args parameters used to call this task
    args: typing.List[typing.Any]

    # keyword arguements parameter used to call this task
    kwargs: typing.MutableMapping[str, typing.Any]

    # How many times the current task has been retries
    retries: int = 0

    # the unique id of the task's group, if this task is a member.
    group: typing.Optional[str] = None

    # Name of the host that sent this task
    origin: typing.Optional[str] = None

    # The original ETA of the task
    eta: typing.Optional[datetime] = None

    # The original expiration of the task
    expires: typing.Optional[datetime] = None

    # Node name of the worker instance executing the task
    hostname: typing.Optional[str] = None

    # Where to send reply to (queue name)
    reply_to: typing.Optional[str] = None

    # time limit
    time_limit: typing.Optional[int] = None

    # A list of signatures to call if the task exited successfully.
    callbacks: typing.Optional[typing.List[typing.Any]] = None

    # errbacks
    errbacks: typing.Optional[typing.List[typing.Any]] = None

    # A list of signatures that only executes after all of the tasks in a group has finished executing.
    chord: typing.Optional[typing.List[typing.Any]] = None

    # chain
    chain: typing.Optional[typing.List[typing.Any]] = None

    @classmethod
    def from_message(cls, app, message: Message):
        if message.headers.get("id", None):
            return cls.from_message_v2(app, message)
        return cls.from_message_v1(app, message)

    @classmethod
    def from_message_v2(cls, app, message: Message):
        args, kwargs, embeds = message.decode()

        eta = maybe_iso8601(message.headers.get("eta", None))
        expires = maybe_iso8601(message.headers.get("expires", None))

        return Request(
            app=app,
            id=message.headers.get("id", None),
            correlation_id=message.properties.get("correlation_id", None),
            args=args,
            kwargs=kwargs,
            retries=try_to_int(message.headers.get("retries", 0)),
            eta=eta,
            expires=expires,
            group=message.headers.get("group", None),
            origin=message.headers.get("origin", None),
            reply_to=message.properties.get("reply_to", None),
            time_limit=get_time_limit(message.headers.get("time_limit", None)),
            chord=embeds.get("chord", None),
            callbacks=embeds.get("callbacks", None),
            errbacks=embeds.get("errbacks", None),
            chain=embeds.get("chain", None),
        )

    @classmethod
    def from_message_v1(cls, app, message: Message):
        """All information for v1 is in message body"""
        task_message = MessageBodyV1.from_dict(message.decode())

        return Request(
            app=app,
            id=task_message.id,
            correlation_id=task_message.id,
            args=task_message.args,
            kwargs=task_message.kwargs,
            retries=task_message.retries,
            eta=task_message.eta,
            expires=task_message.expires,
            group=task_message.taskset,
            time_limit=get_time_limit(task_message.timelimit),
            chord=task_message.chord,
            callbacks=task_message.callbacks,
            errbacks=task_message.errbacks,
        )

    @property
    def is_delayed(self):
        """Check if the request has a future ETA"""
        return self.eta is not None

    def countdown(self, now=None):
        """Get the TTL in seconds if the task has a future ETA."""
        if not self.eta:
            return None
        now = now or self.app.now
        countdown = (self.eta - now()).total_seconds()
        return None if countdown < 0 else countdown

    def is_expired(self, now=None):
        """Check if the request is expired"""
        if self.expires is None:
            return False
        now = now or self.app.now
        return (now() - self.expires).total_seconds() >= 0


def get_time_limit(time_limit):
    if not time_limit:
        return time_limit

    if isinstance(time_limit, (int, float)):
        return time_limit

    soft_limit, hard_limit = time_limit

    if soft_limit is not None and hard_limit is not None:
        return min(soft_limit, hard_limit)
    if soft_limit is None:
        return soft_limit
    if hard_limit is None:
        return hard_limit
