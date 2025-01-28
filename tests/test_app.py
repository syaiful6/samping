from datetime import timezone, datetime
import pytest

from .example_app import app, test_task, test_task_v1


async def send(message):
    print(message)


async def receive():
    pass


def test_config_setting():
    tz = app.timezone
    assert isinstance(tz, timezone)


def test_timezone_config():
    app.conf.timezone = "Asia/Jakarta"
    assert app.conf.timezone == "Asia/Jakarta"

    now = app.now()
    assert not now.utcoffset()


@pytest.mark.asyncio
async def test_send_beat_event():
    event = {
        "type": "beat",
        "time": datetime.now(timezone.utc).isoformat(),
    }

    await app(event, receive, send)


@pytest.mark.asyncio
async def test_send_queue_event():
    msg = test_task.to_message(args=["argument-data"], expires=120)
    event = {
        "type": "queue",
        "headers": msg.headers,
        "properties": msg.properties,
        "content_type": msg.content_type,
        "content_encoding": msg.content_encoding,
        "hostname": "",
        "body": msg.body,
    }

    await app(event, receive, send)


@pytest.mark.asyncio
async def test_send_queue_event_v1():
    msg = test_task_v1.to_message(args=[5], expires=120)
    event = {
        "type": "queue",
        "headers": msg.headers,
        "properties": msg.properties,
        "content_type": msg.content_type,
        "content_encoding": msg.content_encoding,
        "hostname": "",
        "body": msg.body,
    }

    await app(event, receive, send)
