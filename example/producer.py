import argparse
import asyncio
import uuid
from .app import test_task, buggy_task


def run_producer(number):
    async def producer():
        await asyncio.gather(
            *[test_task.apply(kwargs={"data": uuid.uuid4().hex}) for number in range(number)]
        )

        await buggy_task.apply(args=[10])

    asyncio.run(producer())


if __name__ == "__main__":
    parser = argparse.ArgumentParser("example producer")

    parser.add_argument("--num-messages", type=int, default=3)

    args = parser.parse_args()

    run_producer(args.num_messages)
