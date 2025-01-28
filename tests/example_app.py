import asyncio
import logging

from samping.driver.sqs import SQSDriver
from samping import App, Rule, ProtocolVersion

logger = logging.getLogger("example")


def driver():
    return SQSDriver(
        endpoint_url="http://localhost:9324",
        use_ssl=False,
        prefetch_size=30,
        visibility_timeout=60,
    )


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


@app.task(name="buggy_task", protocol_version=ProtocolVersion.V1)
async def test_task_v1(wait: int):
    logger.info("test_task_v1 called with wait: %d", wait)
    await asyncio.sleep(wait)
    return f"waiting completed: {wait}"


@app.tab(name="print_each_minute")
async def print_each_minute():
    logger.info("crontab print each minute executed")
    await asyncio.sleep(2)
