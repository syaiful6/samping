import base64
import aioboto3
from itertools import zip_longest, count
from datetime import timedelta
from typing import (
    Any,
    AsyncGenerator,
    List,
    Iterable,
    TypeVar,
    Optional,
)
import logging
import asyncio
import json
from botocore.exceptions import ClientError


from ..messages import Message
from . import QueueDriver
from ..utils.iterators import countdown
from ..utils.format import try_to_int
from ..utils.time import (
    get_exponential_backoff_interval,
    maybe_eta_delay_seconds,
    utcnow,
)
from ..utils.json import loads, dumps
from ..utils.encoding import str_to_bytes
from ..utils.btree import Btree

T = TypeVar("T")

message_counter = count()


def less_message(a, b):
    delivery_a = a.properties.get("delivery_info", {})
    delivery_b = b.properties.get("delivery_info", {})

    if delivery_a.get("delivered_at", None) == delivery_b.get("delivered_at", None):
        return delivery_a.get("message_id", 0) < delivery_b.get("message_id", 0)

    return delivery_a.get("delivered_at", None) < delivery_b.get("delivered_at", None)


class Qos:
    def __init__(self, prefetch_count=50, visibility_timeout: int = 300):
        self.prefetch_count = prefetch_count
        self._delivered = Btree(100, less=less_message)
        self.visibility_timeout = visibility_timeout

    def append(self, message):
        self._delivered.set(message)

    def ack(self, message):
        self._delivered.remove(message)

    def can_consume(self):
        pcount = self.prefetch_count
        return not pcount or len(self._delivered) < pcount

    def __len__(self):
        return len(self._delivered)

    def can_consume_max_estimate(self):
        pcount = self.prefetch_count
        return max(pcount - len(self._delivered), 0)

    def get_unacked_messages(self):
        pivot = Message(
            "",
            properties={
                "delivery_info": {
                    "message_id": next(message_counter),
                    "delivered_at": utcnow()
                    - timedelta(seconds=self.visibility_timeout),
                }
            },
        )
        return list(self._delivered.descend(pivot))

    def remove_unacked_messages(self):
        for message in self.get_unacked_messages():
            self._delivered.remove(message)


def chunk(iterable: Iterable[T], n: int) -> Iterable[List[T]]:
    args = [iter(iterable)] * n
    grouper = zip_longest(*args, fillvalue=None)
    for chunk in grouper:
        yield list(filter(None, chunk))


class SQSDriver(QueueDriver):
    def __init__(
        self,
        visibility_timeout: int = 300,
        prefetch_size: int = 10,
        batch_window: int = 10,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        aws_session_token: Optional[str] = None,
        region_name: Optional[str] = None,
        botocore_session: Optional[Any] = None,
        profile_name: Optional[str] = None,
        use_ssl: bool = True,
        verify: Optional[bool] = None,
        endpoint_url: Optional[str] = None,
    ):
        self.session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            botocore_session=botocore_session,
            profile_name=profile_name,
        )
        self.use_ssl = use_ssl
        self.verify = verify
        self.endpoint_url = endpoint_url
        self.visibility_timeout = visibility_timeout
        self.lock = asyncio.Lock()
        self._queues = {}
        self.logger = logging.getLogger("samping")
        self.qos = Qos(
            prefetch_count=prefetch_size, visibility_timeout=visibility_timeout
        )
        self.batch_window = batch_window

    def sqs_client(self):
        return self.session.resource(
            "sqs",
            use_ssl=self.use_ssl,
            verify=self.verify,
            endpoint_url=self.endpoint_url,
        )

    async def get_queue(self, sqs: Any, queue: str):
        async with self.lock:
            if queue in self._queues:
                return await sqs.Queue(self._queues[queue])
            try:
                sqs_queue = await sqs.get_queue_by_name(QueueName=queue)
            except ClientError as error:
                if error.response["Error"]["Code"] in [
                    "AWS.SimpleQueueService.NonExistentQueue",
                    "QueueDoesNotExist",
                ]:
                    # create queue
                    sqs_queue = await sqs.create_queue(
                        QueueName=queue,
                    )
                else:
                    raise error
            self._queues[queue] = sqs_queue.url
            return sqs_queue

    async def send_batch(self, queue: str, messages: List[Message], **kwargs):
        return await self._send_batch(queue, messages)

    async def _send_batch(self, queue: str, messages: List[Message]):
        async with self.sqs_client() as sqs:
            sqs_queue = await self.get_queue(sqs, queue)
            async with BatchWriter(sqs_queue) as batch:
                for id, message in enumerate(messages):
                    await batch.send_message(self.message_to_sqs(message, str(id)))

    async def _fetch_messages_on(self, queues: List[str]):
        async with self.sqs_client() as sqs:
            messages = []
            i = 0
            while i < len(queues):
                sqs_queue = await self.get_queue(sqs, queues[i])
                for max_num in countdown(
                    self.qos.can_consume_max_estimate(),
                    min(self.qos.can_consume_max_estimate(), 10),
                ):
                    current_sqs_messages = await sqs_queue.receive_messages(
                        AttributeNames=["All"],
                        MaxNumberOfMessages=max_num,
                        MessageAttributeNames=["All"],
                        VisibilityTimeout=self.visibility_timeout,
                        WaitTimeSeconds=self.batch_window,
                    )
                    if not current_sqs_messages:
                        break
                    current_messages = await asyncio.gather(
                        *[
                            self._message_to_python(message, queues[i])
                            for message in current_sqs_messages
                        ]
                    )
                    messages.extend(current_messages)
                # break if we already have message, otherwise try next queue
                if not messages:
                    i += 1
                else:
                    break
            return messages

    async def consume(self, queues: str) -> AsyncGenerator[Message, None]:
        queue_names = list(filter(lambda x: x, queues.split(",")))
        retries = 0
        while True:
            next_wait = 0
            if self.qos.can_consume():
                messages = await self._fetch_messages_on(queue_names)
                if messages:
                    self.logger.debug("sqs: received %d messages", len(messages))
                    retries = 0
                    async with self.lock:
                        for message in messages:
                            self.qos.append(message)
                            yield message
                else:
                    retries += 1
                    next_wait = get_exponential_backoff_interval(2, retries, 120)
                    self.logger.info(
                        "sqs: received empty messages, in flight messages: %d",
                        len(self.qos),
                    )
            else:
                retries += 1
                next_wait = get_exponential_backoff_interval(2, retries, 120)
                self.logger.info(
                    "waiting application to process messages, in flight messages: %s",
                    len(self.qos),
                )
                async with self.lock:
                    self.qos.remove_unacked_messages()

            self.logger.debug(
                f"sqs: sleeping for {next_wait} seconds before polling again"
            )
            await asyncio.sleep(next_wait or self.batch_window)

    async def send_ack(self, message: Message):
        ack_id = message.properties.get("delivery_tag", None)
        delivery_info = message.properties.get("delivery_info", {})
        queue = delivery_info.get("routing_key", None)

        if queue and ack_id:
            async with self.sqs_client() as sqs:
                sqs_queue = await self.get_queue(sqs, queue)
                sqs_message = await sqs_queue.Message(ack_id)
                await sqs_message.delete()

            async with self.lock:
                self.qos.ack(message)
        else:
            self.logger.debug(
                "message have empty delivery_tag and delivery_info properties"
            )

    async def send_nack(self, message: Message, delay: int = 1):
        ack_id = message.properties.get("delivery_tag", None)
        delivery_info = message.properties.get("delivery_info", {})
        queue = delivery_info.get("routing_key", None)

        if queue and ack_id:
            async with self.sqs_client() as sqs:
                sqs_queue = await self.get_queue(sqs, queue)
                sqs_message = await sqs_queue.Message(ack_id)
                await sqs_message.change_visibility(VisibilityTimeout=delay)

            async with self.lock:
                self.qos.ack(message)
        else:
            self.logger.debug(
                "message have empty routing_key and delivery_info properties"
            )

    async def _message_to_python(self, sqs_message, queue: str) -> Message:
        bodyStr = await sqs_message.body
        attributes = await sqs_message.attributes

        delivery_info = {
            "routing_key": queue,  # set the queue as routing key
            "approximate_receive_count": try_to_int(
                attributes.get("ApproximateReceiveCount", 1), 1
            ),
            "message_id": next(message_counter),
            "delivered_at": utcnow(),
        }
        message_attributes = {
            "delivery_info": delivery_info,
            "delivery_tag": sqs_message.receipt_handle,
        }
        try:
            message = loads(bodyStr)
        except json.decoder.JSONDecodeError:
            body = base64.standard_b64decode(bodyStr)
            return Message(
                body,
                headers={},
                properties=message_attributes,
            )
        else:
            headers = message.get("headers", {})
            body = base64.standard_b64decode(message.get("body", None))
            properties = message.get("properties", {})
            properties.update(message_attributes)
            return Message(
                body,
                headers=headers,
                properties=properties,
                content_type=message.get("content_type", None),
                content_encoding=message.get("content_encoding", None),
            )

    def _to_delivery_dict(self, message: Message):
        body = base64.standard_b64encode(str_to_bytes(message.body)).decode()
        properties = message.properties or {}
        if "delivery_info" in properties:
            del properties["delivery_info"]
        if "delivery_tag" in properties:
            del properties["delivery_tag"]

        return {
            "headers": message.headers,
            "properties": properties,
            "content_type": message.content_type,
            "content_encoding": message.content_encoding,
            "body": body,
        }

    def message_to_sqs(self, message: Message, message_id=None):
        sqs_message = {
            "MessageBody": dumps(self._to_delivery_dict(message)),
        }
        if message_id:
            sqs_message["Id"] = message_id

        delay = maybe_eta_delay_seconds(message.headers.get("eta", None))
        if delay:
            # maximum delay for sqs is 15 minute
            sqs_message["DelaySeconds"] = max(0, min(delay, 900))

        return sqs_message

    async def size(self, queue: str) -> int:
        async with self.sqs_client() as sqs:
            sqs_queue = await self.get_queue(sqs, queue)
            attributes = await sqs_queue.attributes
            return attributes.get("ApproximateNumberOfMessages", 0)


class BatchWriter:
    def __init__(self, queue, flush_amount=10, flush_size=250000):
        self._queue = queue
        self._messages = []
        self._current_size = 0
        self._flush_size = flush_size
        self._flush_amount = flush_amount
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        if self._messages:
            await self._flush()

    def get_size(self, message):
        body = message.get("MessageBody", "").encode("utf-8")
        return len(body)

    async def send_message(self, message):
        async with self._lock:
            current_size = self._current_size + self.get_size(message)
            if current_size > self._flush_size:
                await self._flush()
                self._messages.append(message)
                self._current_size += self.get_size(message)
                if len(self._messages) >= self._flush_amount:
                    await self._flush()
            else:
                self._current_size = current_size
                self._messages.append(message)
                if len(self._messages) >= self._flush_amount:
                    await self._flush()

    async def _flush(self):
        response = await self._queue.send_messages(Entries=self._messages)
        self._messages = []
        if len(response.get("Failed", [])) > 0:
            first = response["Failed"][0]
            raise Exception(
                "sqs send_batch: failed for %d message(s): %s"
                % (len(response["Failed"]), first.get("Message", ""))
            )
        self._current_size = 0
