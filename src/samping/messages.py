import typing
import number
from dataclasses import dataclass
import msgpack
from datetime import datetime
from collections import namedtuple

from .serialization import loads, dumps
from .exceptions import DecodeError
from .utils.format import to_iso_format, parse_iso8601


@dataclass
class Message:
    body: str
    delivery_tag: typing.Optional[str] = None
    content_type: typing.Optional[str] = None
    content_encoding: typing.Optional[str] = None
    headers: typing.Optional[typing.Dict[str, str]] = None
    properties: typing.Optional[typing.Dict[str, str]] = None
    delivery_info: typing.Optional[typing.Dict[str, str]] = None
    accept: typing.Optional[typing.List[str]] = None

    def decode(self):
        try:
            return loads(self.body, self.content_type, self.content_encoding, self.accept)
        except DecodeError:
            # the legacy one use msgpack
            return loads(self.body,  "application/x-msgpack", "binary")


@dataclass
class TaskMessageV1:
    id: str
    task: str
    args: typing.Optional[typing.List[typing.Any]] = None
    kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None
    retries: int = 3
    eta: typing.Optional[datetime] = None
    expires: typing.Optional[datetime] = None

    @classmethod
    def from_dict(cls, messages):
        eta = messages.get("eta", None)
        expires = messages.get("expires", None)
        if eta and isinstance(eta, str):
            messages["eta"] = parse_iso8601(eta)
        if expires and isinstance(expires, str):
            messages["expires"] = parse_iso8601(expires)
        return cls(**messages)

    def encode(self):
        return dumps({
            "id": self.id,
            "task": self.task,
            "args": self.args,
            "kwargs": self.kwargs,
            "retries": self.retries,
            "eta": to_iso_format(self.eta) if self.eta else None,
            "expires": to_iso_format(self.expires) if self.expires else None,
        }, "msgpack")

        return data


TaskMessageBody = namedtuple("MessageBody", ("args", "kwargs", "childrens"))


@dataclass
class TaskMessageV2:
    headers: typing.Dict[str, str]
    properties: typing.Dict[str, str]
    body: MessageBody

    def encode(self, serializer):
        return dumps({
            "headers": self.headers,
            "properties": self.properties,
            "body": self.body,
        }, serializer)


TaskMessage = TaskMessageV2