import logging

from ..driver import QueueDriver
from ..messages import Message
from ..types import ASGI3Application
from ..channel import Sender
from ..tasks.tasks import TaskStatus


logger = logging.getLogger("samping")


class QueueCycle:
    def __init__(
        self, message: Message, driver: QueueDriver, sender: Sender[TaskStatus]
    ):
        self.message = message
        self.driver = driver
        self.sender = sender

    def convert_queue_to_protocol(self):
        delivery_info = self.message.properties.get("delivery_info", {})
        headers = {
            "content_type": self.message.content_type,
            "content_encoding": self.message.content_encoding,
        }
        headers.update(self.message.headers or {})
        return {
            "type": "queue",
            "name": delivery_info.get("routing_key", ""),
            "headers": [(k, v) for k, v in headers.items()],
            "properties": [
                (k, v)
                for k, v in self.message.properties.items()
                if k not in ["delivery_info", "delivery_tag"]
            ]
            if self.message.properties
            else [],
            "body": self.message.body,
            "broker": "",
        }

    async def run_app(self, app: ASGI3Application):
        try:
            await self.sender.send(TaskStatus.PENDING)
            scope = self.convert_queue_to_protocol()
            await app(scope, self.receive, self.send)
        except BaseException as exc:
            msg = "Exception in Samping application\n"
            logger.error(msg, exc_info=exc)
        finally:
            await self.sender.send(TaskStatus.FINISHED)

    async def receive(self):
        pass

    async def send(self, message):
        assert message["type"] in ["queue.ack", "queue.nack"]
        if message["type"] == "queue.ack":
            return await self.driver.send_ack(self.message)
        elif message["type.nack"] == "queue.nack":
            return await self.driver.send_nack(self.message, message.get("delay", 0))
