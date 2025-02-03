import typing
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .serialization import loads, dumps
from .exceptions import DecodeError
from .utils.time import maybe_iso8601


class ProtocolVersion(Enum):
    V1 = "v1"
    V2 = "v2"


@dataclass
class Message:
    body: str
    content_type: typing.Optional[str] = None
    content_encoding: typing.Optional[str] = None
    headers: typing.Optional[typing.Dict[str, str]] = None
    properties: typing.Optional[typing.Dict[str, str]] = None
    accept: typing.Optional[typing.List[str]] = None

    def decode(self):
        try:
            return loads(
                self.body, self.content_type, self.content_encoding, self.accept
            )
        except DecodeError:
            # the legacy one use msgpack
            return loads(self.body, "application/x-msgpack", "binary")


@dataclass
class MessageBodyV1:
    id: str
    task: str
    args: typing.Optional[typing.List[typing.Any]] = None
    kwargs: typing.Optional[typing.Dict[str, typing.Any]] = None
    retries: int = 3
    eta: typing.Optional[datetime] = None
    expires: typing.Optional[datetime] = None
    taskset: typing.Optional[str] = None
    chord: typing.Optional[typing.List[typing.Any]] = None
    utc: bool = True
    callbacks: typing.Optional[typing.List[typing.Any]] = None
    errbacks: typing.Optional[typing.List[typing.Any]] = None
    timelimit: typing.Optional[typing.Tuple[float, float]] = None

    @classmethod
    def from_dict(cls, messages):
        eta = messages.get("eta", None)
        expires = messages.get("expires", None)
        if eta and isinstance(eta, str):
            messages["eta"] = maybe_iso8601(eta)
        if expires and isinstance(expires, str):
            messages["expires"] = maybe_iso8601(expires)
        return cls(**messages)

    def encode(self):
        return dumps(
            {
                "id": self.id,
                "task": self.task,
                "args": self.args,
                "kwargs": self.kwargs,
                "retries": self.retries,
                "eta": self.eta.isoformat() if self.eta else None,
                "expires": self.expires.isoformat() if self.expires else None,
            },
            "msgpack",
        )


@dataclass
class MessageBodyV2:
    args: typing.List[typing.Any]
    kwargs: typing.Dict[str, typing.Any]
    embeds: typing.Dict[str, typing.Any]

    def encode(self, serializer):
        return dumps((self.args, self.kwargs, self.embeds), serializer)


TaskMessage = MessageBodyV2
