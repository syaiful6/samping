from samping import LambdaAdapter
from samping.driver.sqs import SQSDriver
import uuid

from .example_app import app, test_task, test_task_v1


def test_execute_sqs_event(mocker):
    driver = SQSDriver(
        endpoint_url="http://localhost:9324",
        use_ssl=False,
        prefetch_size=15,
        visibility_timeout=60,
    )
    message = test_task_v1.to_message(args=[10])
    message_v2 = test_task.to_message(args=["lambda-test"])
    sqs_message1 = driver.message_to_sqs(message)
    sqs_message2 = driver.message_to_sqs(message_v2)

    spy = mocker.spy(app.driver, "send_ack")

    # lambda message event
    event = {
        "Records": [
            {
                "messageId": uuid.uuid4().hex,
                "receiptHandle": uuid.uuid4().hex,
                "attributes": {
                    "ApproximateReceiveCount": "2",
                },
                "body": m["MessageBody"],
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:samping",
                "awsRegion": "us-east-2",
            }
            for m in [sqs_message1, sqs_message2]
        ]
    }
    lambda_handler = LambdaAdapter(app)

    lambda_handler(event, {})

    assert spy.call_count == 2


def test_execute_event_bridget_schedule(mocker):
    # lambda eventbridget event structure
    event = {
        "version": "0",
        "id": "53dc4d37-cffa-4f76-80c9-8b7d4a4d2eaa",
        "detail-type": "Scheduled Event",
        "source": "aws.events",
        "account": "123456789012",
        "time": "2024-11-08T16:53:06Z",
        "region": "us-east-1",
        "resources": ["arn:aws:events:us-east-1:123456789012:rule/my-scheduled-rule"],
        "detail": {},
    }

    spy = mocker.spy(app, "beat")

    lambda_handler = LambdaAdapter(app)
    lambda_handler(event, {})

    assert spy.call_count == 1
