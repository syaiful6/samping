from samping.messages import Message
from samping.serialization import dumps
from samping.utils.format import utcnow
from samping.utils.compat import msgpack_dumps

def test_encode_task_message():
    task = {
        "id": 9023,
        "expires": utcnow()
    }
    content_type, content_encoding, data = dumps(task)

    message = Message(data, "delivery_tag", content_type=content_type, content_encoding=content_encoding)
    decoded = message.decode()

    assert task.get("id") == decoded.get("id")


def test_encode_task_legacy_message():
    task = {
        "id": 9023,
        "expires": utcnow()
    }
    body = msgpack_dumps(task)

    message = Message(body, "delivery_tag")
    decoded = message.decode()

    assert task.get("id") == decoded.get("id")