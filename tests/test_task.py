from .example_app import test_task, test_task_v1
from samping.tasks.request import Request
import pytest


def test_task_as_message():
    t = test_task.to_message(args=["argument-data"], expires=120)
    task_message = t.decode()
    # in v2, we embed list
    assert isinstance(task_message, list)


def test_as_task_v2():
    t = test_task.as_message_v2("id", test_task.name, ["argument-data"])
    assert t.headers.get("retries", 3) == test_task.max_retries
    assert t.headers.get("id") == "id"


def test_create_request_from_message():
    msg = test_task.to_message(args=["argument-data"], expires=120)
    request = Request.from_message(test_task.app, msg)

    assert request.expires is not None
    assert request.args == ["argument-data"]


def test_as_task_v1():
    test = test_task_v1.to_message(args=[30], expires=30)
    assert test.headers.get("id", None) is None


def test_create_request_from_message_v1():
    msg = test_task_v1.to_message(args=[30], expires=30)
    request = Request.from_message(test_task_v1.app, msg)

    assert request.args == [30]
    assert request.expires is not None


@pytest.mark.asyncio
async def test_create_task_executiong_v1():
    msg = test_task_v1.to_message(args=[3], expires=30)
    task = test_task_v1.from_request(Request.from_message(test_task_v1.app, msg))

    assert task.name == test_task_v1.name
    assert task.run == test_task_v1.run
    assert task.request is not None

    assert test_task_v1.request is None

    assert task.app is not None

    result = await task.run(*task.request.args, **task.request.kwargs)

    assert result == "waiting completed: 3"
