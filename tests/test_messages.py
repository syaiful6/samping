import datetime

from samping.messages import Message, MessageBodyV1, MessageBodyV2
from samping.serialization import dumps
from samping.utils.time import utcnow
from samping.utils.compat import msgpack_dumps


def test_encode_task_message():
    task = {"id": 9023, "expires": utcnow()}
    content_type, content_encoding, data = dumps(task)

    message = Message(
        data,
        content_type=content_type,
        content_encoding=content_encoding,
        headers={},
    )
    decoded = message.decode()

    assert task.get("id") == decoded.get("id")


def test_encode_task_legacy_message():
    task = {"id": 9023, "expires": utcnow()}
    body = msgpack_dumps(task)

    message = Message(body)
    decoded = message.decode()

    assert task.get("id") == decoded.get("id")


def test_encode_task_messagev1_with_eta():
    task_message = MessageBodyV1(
        id="id", task="test_encode", eta=datetime.datetime.now()
    )
    content_type, content_encoding, body = task_message.encode()
    assert isinstance(body, bytes)
    message = Message(
        body=body, content_type=content_type, content_encoding=content_encoding
    )
    task_message_2 = MessageBodyV1.from_dict(message.decode())

    assert isinstance(task_message_2.eta, datetime.datetime)


def test_encode_task_message_v2():
    task_message = MessageBodyV2(
        ["data"],
        {},
        {"callbacks": None, "errbacks": None, "chain": None, "chord": None},
    )
    content_type, encoding, data = task_message.encode("json")

    assert content_type == "application/json"
    assert encoding == "utf-8"
    assert isinstance(data, str)
