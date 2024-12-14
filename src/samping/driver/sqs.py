import base64
import aioboto3
from itertools import zip_longest
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
from botocore.exceptions import ClientError


from ..messages import Message
from ..backoff import Backoff, Exponential
from . import QueueDriver
from ..utils import countdown, try_to_int

T = TypeVar("T")


def chunk(iterable: Iterable[T], n: int) -> Iterable[List[T]]:
    args = [iter(iterable)] * n
    grouper = zip_longest(*args, fillvalue=None)
    for chunk in grouper:
        yield list(filter(None, chunk))


class SQSDriver(QueueDriver):
    def __init__(
        self,
        backoff: Optional[Backoff] = None,
        visibility_timeout: int = 30,
        batch_window: int = 5,
        batch_size: int = 10,
        prefetch_size: int = 50,
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
        self._backoff = backoff or Exponential()
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
        self.batch_window = batch_window
        self.batch_size = min(batch_size, 20)
        self.logger = logging.getLogger("samping")
        self._in_flight = 0
        self._prefetch_size = prefetch_size

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
                    body = base64.standard_b64encode(message.body).decode()
                    entry = {
                        "Id": str(id),
                        "MessageBody": body,
                    }
                    await batch.send_message(entry)

    async def _send_batch_chunk(self, queue: str, messages: List[Message]):
        async with self.sqs_client() as sqs:
            entries = []
            for id, message in enumerate(messages):
                body = base64.standard_b64encode(message.body).decode()
                entry = {
                    "Id": str(id),
                    "MessageBody": body,
                }
                entries.append(entry)
            sqs_queue = await self.get_queue(sqs, queue)
            response = await sqs_queue.send_messages(Entries=entries)
            if len(response.get("Failed", [])) > 0:
                first = response["Failed"][0]
                raise Exception(
                    "sqs send_batch: failed for %d message(s): %s"
                    % (len(response["Failed"]), first.get("Message", ""))
                )

    async def _fetch_messages_on(self, queues: List[str]):
        async with self.sqs_client() as sqs:
            messages = []
            for queue in queues:
                sqs_queue = await self.get_queue(sqs, queue)
                if len(messages) >= self.batch_size:
                    break
                for max_num in countdown(
                    max(self._prefetch_size - self._in_flight, 10),
                    min(self.batch_size, 10),
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
                            self.decode_message(message, queue)
                            for message in current_sqs_messages
                        ]
                    )
                    messages.extend(current_messages)
            return messages

    async def consume(self, queues: str) -> AsyncGenerator[Message, None]:
        queue_names = list(filter(lambda x: x, queues.split(",")))
        while True:
            next_wait = 0
            if self._in_flight < self._prefetch_size:
                messages = await self._fetch_messages_on(queue_names)
                if messages:
                    self._backoff.reset()
                    self._in_flight += len(messages)
                    for message in messages:
                        yield message
                else:
                    next_wait = self._backoff.next_backoff().total_seconds()
                    self.logger.info("sqs: got empty messages")
            else:
                next_wait = self._backoff.next_backoff().total_seconds()
                self.logger.debug(
                    f"waiting in {self._in_flight} flight message to be processed (max: {self._prefetch_size})"
                )

            if next_wait == 0:
                self._backoff.reset()
                next_wait = self.batch_window
            self.logger.debug(
                f"sqs: sleeping for {next_wait} seconds before polling again"
            )
            await asyncio.sleep(next_wait)

    async def send_ack(self, ack_id: str, queue: str = None):
        async with self.sqs_client() as sqs:
            sqs_queue = await self.get_queue(sqs, queue)
            message = await sqs_queue.Message(ack_id)
            await message.delete()
            self._in_flight = max(0, self._in_flight - 1)

    async def send_nack(self, ack_id: str, queue: str = None, delay: int = 1):
        async with self.sqs_client() as sqs:
            sqs_queue = await self.get_queue(sqs, queue)
            message = await sqs_queue.Message(ack_id)
            await message.change_visibility(VisibilityTimeout=delay)
            self._in_flight = max(0, self._in_flight - 1)

    async def decode_message(self, sqs_message, queue: str) -> Message:
        bodyStr = await sqs_message.body
        body = base64.standard_b64decode(bodyStr)
        attributes = await sqs_message.attributes
        return Message(
            sqs_message.receipt_handle,
            body,
            queue,
            try_to_int(attributes.get("ApproximateReceiveCount", 1), 1),
        )

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
