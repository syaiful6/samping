import argparse
import logging
import sys
import asyncio

from .app import app


def run_worker(queues: str, num_worker: int):
    root = logging.getLogger()
    root.setLevel(logging.INFO)

    logging.getLogger("samping").setLevel(logging.DEBUG)
    logging.getLogger("example").setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    root.addHandler(handler)

    asyncio.run(app.run_worker(queues, num_worker))


if __name__ == "__main__":
    parser = argparse.ArgumentParser("serpengine worker")
    parser.add_argument("--queue", help="queue to process", default="samping")
    parser.add_argument("--num-worker", type=int, default=3)

    args = parser.parse_args()

    run_worker(args.queue, args.num_worker)
