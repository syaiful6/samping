from datetime import datetime, date
import uuid
import decimal
from typing import List, Dict, Any, Optional
import msgpack

from .utils import parse_iso8601, to_iso_format


def encode_non_standard_msgpack(obj):
    if isinstance(obj, (decimal.Decimal, uuid.UUID)):
        return str(obj)
    if isinstance(obj, (datetime, date)):
        if not isinstance(obj, datetime):
            obj = datetime(obj.year, obj.month, obj.day, 0, 0, 0, 0)
        r = obj.isoformat()
        if r.endswith("+00:00"):
            r = r[:-6] + "Z"
        return r
    return obj


def msgpack_dumps(s, **kwargs):
    return msgpack.packb(s, default=encode_non_standard_msgpack, use_bin_type=True)


class Message:
    __slots__ = (
        "ack_id",
        "body",
        "queue",
        "receive_count",
    )

    def __init__(
        self, ack_id: str, body: bytes, queue: str = "", receive_count: int = 1
    ):
        self.ack_id = ack_id
        self.body = body
        self.queue = queue
        self.receive_count = receive_count

    def decode_task_message(self):
        messages = msgpack.unpackb(self.body, raw=False)
        eta = messages.get("eta", None)
        expires = messages.get("expires", None)
        if eta and isinstance(eta, str):
            messages["eta"] = parse_iso8601(eta)
        if expires and isinstance(expires, str):
            messages["expires"] = parse_iso8601(expires)
        return TaskMessage(**messages)


class TaskMessage:
    __slots__ = ("id", "task", "args", "kwargs", "retries", "eta", "expires")

    def __init__(
        self,
        id: str,
        task: str,
        args: List[Any] = [],
        kwargs: Dict[str, Any] = {},
        retries: int = 3,
        eta: Optional[datetime] = None,
        expires: Optional[datetime] = None,
    ):
        self.id = id
        self.task = task
        self.args = args
        self.kwargs = kwargs
        self.retries = retries
        self.eta = eta
        self.expires = expires

    def encode(self) -> bytes:
        return msgpack_dumps(
            {
                "id": self.id,
                "task": self.task,
                "args": self.args,
                "kwargs": self.kwargs,
                "retries": self.retries,
                "eta": to_iso_format(self.eta) if self.eta else None,
                "expires": to_iso_format(self.expires) if self.expires else None,
            }
        )
