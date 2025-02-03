import asyncio
import base64
import logging
import typing
import json

from ..protocol import LifeSpanCyle, QueueCycle, beat_cycle
from ..app import App
from ..messages import Message
from ..utils.format import try_to_int
from ..utils.time import maybe_iso8601, utcnow
from ..utils.json import loads
from ..channel import channel, Sender

logger = logging.getLogger("samping")


class LambdaAdapter:
    def __init__(self, app: "App"):
        self.app = app

    def __call__(self, event: dict, context: dict):
        if self.is_queue_event(event):
            messages = self.decode_sqs_messages(event["Records"])
            self.run_in_event_loop(self.run_queue_cycles(messages))
        elif self.is_scheduled_event(event):
            self.run_in_event_loop(self.run_beat_cycle(event))
        else:
            raise TypeError("Invalid SQS event or EventBridge event")
        
    def run_in_event_loop(self, coro):
        loop = asyncio.get_event_loop()
        task = loop.create_task(coro)
        return loop.run_until_complete(task)

    def is_queue_event(self, event):
        return (
            "Records" in event
            and len(event["Records"]) > 0
            and "body" in event["Records"][0]
        )

    def is_scheduled_event(self, event):
        return event.get("source", "") == "aws.events" and "time" in event

    async def run_queue_cycles(self, messages: typing.List[Message]):
        logger.debug("Received %s messages from SQS", len(messages))
        async with LifeSpanCyle(self.app) as lifespan:
            if lifespan.should_exit:
                return
            ev_tx, _ = channel()

            await asyncio.gather(
                *[self.run_queue_cycle(message, ev_tx) for message in messages]
            )

    async def run_beat_cycle(self, event: dict):
        logger.debug("Received EventBridge scheduled event")
        async with LifeSpanCyle(self.app) as lifespan:
            if lifespan.should_exit:
                return
            event_time = maybe_iso8601(event.get("time", ""))
            if not event_time:
                event_time = utcnow()

            await beat_cycle(self.app, event_time)

    async def run_queue_cycle(self, message: Message, sender: Sender):
        queue = QueueCycle(message, self.app.driver, sender)
        await queue.run_app(self.app)

    def decode_sqs_messages(self, messages: typing.List[dict]) -> typing.List[Message]:
        return [self.decode_sqs_message(message) for message in messages]

    def decode_sqs_message(self, message: dict) -> Message:
        body = message.get("body", "")
        attributes = message.get("attributes", {})

        delivery_info = {
            "routing_key": self.get_queue_name(message),
            "approximate_receive_count": try_to_int(
                attributes.get("ApproximateReceiveCount", 1), 1
            ),
        }
        message_attributes = {
            "delivery_info": delivery_info,
            "delivery_tag": message.get("receiptHandle", ""),
        }
        try:
            task_message = loads(body)
        except json.decoder.JSONDecodeError:
            body = base64.standard_b64decode(body)
            return Message(
                body,
                headers={},
                properties=message_attributes,
            )
        else:
            headers = task_message.get("headers", {})
            body = base64.standard_b64decode(task_message.get("body", None))
            properties = task_message.get("properties", {})
            properties.update(message_attributes)
            return Message(
                body,
                headers=headers,
                properties=properties,
                content_type=task_message.get("content_type", None),
                content_encoding=task_message.get("content_encoding", None),
            )

    def get_queue_name(self, message):
        source_names = message.get("eventSourceARN", "").split(":")
        if source_names:
            return source_names[-1]

        return ""
