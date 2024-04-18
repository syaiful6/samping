import datetime
from samping.messages import Message, TaskMessage


def test_encode_task_message():
    task_message = TaskMessage("id", "test_encode")
    body = task_message.encode()
    message = Message("ack_id", body)
    assert message.queue == ""
    assert message.ack_id == "ack_id"
    task_message_2 = message.decode_task_message()

    assert task_message.id == task_message_2.id
    assert task_message.task == task_message_2.task


def test_encode_message_with_eta():
    task_message = TaskMessage("id", "test_encode", eta=datetime.datetime.now())
    body = task_message.encode()
    message = Message("ack_id", body)
    task_message_2 = message.decode_task_message()

    assert isinstance(task_message_2.eta, datetime.datetime)
