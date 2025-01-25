import typing
from dataclasses import dataclass
import msgpack

from .serialization import loads
from .exceptions import DecodeError



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