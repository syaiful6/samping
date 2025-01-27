from typing import Dict, List, TypedDict, Any, Union
from datetime import datetime, timedelta
from enum import Enum
from collections.abc import Mapping
import uuid
import numbers

from ..exceptions import Retry
from ..utils.time import maybe_make_aware
from ..utils.format import to_iso_format
from ..messages import Message, MessageBodyV1, MessageBodyV2, ProtocolVersion


#: earliest date supported by time.mktime.
INT_MIN = -2147483648


class TaskInfo(TypedDict):
    args: List[Any]
    kwargs: Dict[str, Any]
    task_id: Union[None, str]
    queue: Union[None, str]
    eta: Union[None, datetime]
    expires: Union[None, datetime]


class class_property:
    def __init__(self, getter=None, setter=None):
        if getter is not None and not isinstance(getter, classmethod):
            getter = classmethod(getter)
        if setter is not None and not isinstance(setter, classmethod):
            setter = classmethod(setter)
        self.__get = getter
        self.__set = setter

        info = getter.__get__(object)  # just need the info attrs.
        self.__doc__ = info.__doc__
        self.__name__ = info.__name__
        self.__module__ = info.__module__

    def __get__(self, obj, type=None):
        if obj and type is None:
            type = obj.__class__
        return self.__get.__get__(obj, type)()

    def __set__(self, obj, value):
        if obj is None:
            return self
        return self.__set.__get__(obj)(value)

    def setter(self, setter):
        return self.__class__(self.__get, setter)


class Task:
    _app = None

    name = None

    max_retries = 3

    serializer = None

    protocol_version = ProtocolVersion.V2

    __bound__ = False

    def retry(self, exc=None, when=None, **kwargs):
        raise Retry(exc=exc, when=when, **kwargs)

    @classmethod
    def bind(cls, app):
        cls.__bound__ = True
        cls._app = app

        cls.on_bound(app)

        return app

    @classmethod
    def on_bound(cls, app):
        pass

    @classmethod
    def _get_app(cls):
        if not cls.__bound__:
            # The app property's __set__  method is not called
            # if Task.app is set (on the class), so must bind on use.
            cls.bind(cls._app)
        return cls._app

    app = class_property(_get_app, bind)

    async def run(self, *args, **kwargs):
        raise NotImplementedError("Tasks must define the run method.")

    def to_message(
        self,
        args=None,
        kwargs=None,
        task_id=None,
        eta=None,
        expires=None,
        timezone=None,
        **options,
    ) -> Message:
        if not task_id:
            task_id = str(uuid.uuid4())

        as_message = (
            self.as_message_v2
            if self.protocol_version == ProtocolVersion.V2
            else self.as_message_v1
        )

        return as_message(
            task_id=task_id,
            name=self.name,
            args=args,
            kwargs=kwargs,
            eta=eta,
            expires=expires,
            timezone=timezone,
            retries=self.max_retries,
            **options,
        )

    def as_message_v2(
        self,
        task_id,
        name,
        args=None,
        kwargs=None,
        eta=None,
        expires=None,
        timezone=None,
        **options,
    ) -> Message:
        args = args or ()
        kwargs = kwargs or {}

        if not isinstance(args, (list, tuple)):
            raise TypeError("task args must be a list or tuple")
        if not isinstance(kwargs, Mapping):
            raise TypeError("task keyword arguments must be a mapping")

        if task_id is None:
            task_id = str(uuid.uuid4())

        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, "expires")
            timezone = timezone or self.app.timezone
            expires = maybe_make_aware(
                self.app.now() + timedelta(seconds=expires), tz=timezone
            )

        if not isinstance(eta, str):
            eta = eta and to_iso_format(eta)

        root_id = options.get("root_id", None)
        if not root_id:
            root_id = task_id

        reply_to = options.get("reply_to", "")

        headers = {
            "lang": "py",
            "task": name,
            "id": task_id,
            "eta": eta,
            "expires": expires,
            "group": options.get("group", None),
            "group_id": options.get("group_id", None),
            "retries": options.get("retries", 3),
            "root_id": root_id,
            "parent_id": options.get("parent_id", None),
            "origin": "",
            "ignore_result": options.get("ignore_result", False),
            "replaced_task_nesting": options.get("replaced_task_nesting", 0),
        }

        body = MessageBodyV2(
            args=args,
            kwargs=kwargs,
            embeds={
                "callbacks": options.get("callbacks", None),
                "errbacks": options.get("errbacks", None),
                "chain": options.get("chain", None),
                "chord": options.get("chord"),
            },
        )

        content_type, content_encoding, data = body.encode(self.serializer)

        return Message(
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            properties={
                "correlation_id": task_id,
                "reply_to": reply_to,
            },
            body=data,
        )

    def as_message_v1(
        self,
        task_id,
        name,
        args=None,
        kwargs=None,
        eta=None,
        expires=None,
        timezone=None,
        **options,
    ):
        args = args or ()
        kwargs = kwargs or {}

        if not isinstance(args, (list, tuple)):
            raise TypeError("task args must be a list or tuple")
        if not isinstance(kwargs, Mapping):
            raise TypeError("task keyword arguments must be a mapping")

        if task_id is None:
            task_id = str(uuid.uuid4())

        if isinstance(expires, numbers.Real):
            self._verify_seconds(expires, "expires")
            timezone = timezone or self.app.timezone
            expires = maybe_make_aware(
                self.app.now() + timedelta(seconds=expires), tz=timezone
            )

        if not isinstance(eta, str):
            eta = eta and to_iso_format(eta)

        body = MessageBodyV1(
            id=task_id,
            task=self.name,
            args=args,
            kwargs=kwargs,
            retries=self.max_retries,
            eta=eta,
            expires=expires,
        )
        content_type, content_encoding, data = body.encode()

        return Message(
            headers={},
            content_type=content_type,
            content_encoding=content_encoding,
            body=data,
        )

    async def batch(self, batches: List[TaskInfo], queue: str = None):
        messages = [self.to_message(**batch) for batch in batches]
        return await self._app.send_batch(
            messages, queue=queue or self._app.task_route(self)
        )

    async def apply(
        self,
        args=None,
        kwargs=None,
        task_id: str = None,
        queue=None,
        eta=None,
        expires=None,
        **opts,
    ):
        queue = queue or self._app.task_route(self)
        return await self._app._send_message(
            self.to_message(
                args=args,
                kwargs=kwargs,
                task_id=task_id,
                queue=queue,
                eta=eta,
                expires=expires,
                **opts,
            ),
            queue=queue,
        )

    def _verify_seconds(self, s, what):
        if s < INT_MIN:
            raise ValueError(f"{what} is out of range: {s!r}")
        return s


class CronJob:
    name = None

    expression = "* * * * *"

    async def run(self):
        raise NotImplementedError("CronJob must define the run method.")


class TaskStatus(Enum):
    PENDING = 1
    FINISHED = 2
