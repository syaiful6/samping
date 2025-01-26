from .example_app import test_task


def test_task_as_message():
    t = test_task.to_message(args=["argument-data"], expires=120)
    task_message = t.decode()
    assert "headers" in task_message