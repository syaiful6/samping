import asyncio
import logging

from samping.driver import MockDriver
from samping import App, Rule, ProtocolVersion

logger = logging.getLogger("example")


def driver():
    return MockDriver()


app = App(
    driver_factory=driver,
    default_queue="samping",
)
app.routes = [
    Rule("test_*", "samping"),
    Rule("buggy_*", "buggy"),
]


@app.task(name="test_task")
async def test_task(data: str):
    logger.info("get test_task with data: %s", data)
    await asyncio.sleep(10)


@app.task(name="test_task_v1", protocol_version=ProtocolVersion.V1)
async def test_task_v1(wait: int):
    logger.info("test_task_v1 called with wait: %d", wait)
    await asyncio.sleep(wait)
    return f"waiting completed: {wait}"


@app.task(name="buggy_task")
async def buggy_task(wait: int):
    logger.info("get buggy_task with wait: %d", wait)
    await asyncio.sleep(wait)
    raise RuntimeError("buggy")


@app.tab(name="print_each_minute")
async def print_each_minute():
    logger.info("crontab print each minute executed")
    await asyncio.sleep(2)
