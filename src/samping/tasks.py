from typing import Dict, List, TypedDict, Any, Union
from datetime import datetime
import uuid

from .exceptions import Retry
from .messages import Message, TaskMessage


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
        args=[],
        kwargs={},
        task_id: str = None,
        queue=None,
        eta=None,
        expires=None,
    ) -> Message:
        if task_id is None:
            task_id = str(uuid.uuid4())
        task_message = TaskMessage(
            task_id, self.name, args, kwargs, self.max_retries, eta, expires
        )
        return Message("", task_message.encode(), queue)

    async def batch(self, batches: List[TaskInfo], queue: str = None):
        messages = [self.to_message(**batch) for batch in batches]
        return await self._app.send_batch(
            messages, queue=queue or self._app.task_route(self)
        )

    async def apply(
        self,
        args=[],
        kwargs={},
        task_id: str = None,
        queue=None,
        eta=None,
        expires=None,
        **opts,
    ):
        return await self._app._send_message(
            self.to_message(
                args=args,
                kwargs=kwargs,
                task_id=task_id,
                queue=queue or self._app.task_route(self),
                eta=eta,
                expires=expires,
            ),
            queue=None,
            **opts,
        )


class CronJob:
    name = None

    expression = "* * * * *"

    async def run(self):
        raise NotImplementedError("CronJob must define the run method.")
