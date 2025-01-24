from samping.driver.sqs import SQSDriver
from samping.app import App
import asyncio
import logging

logger = logging.getLogger("example")


def driver():
    return SQSDriver(
        endpoint_url="http://localhost:9324", use_ssl=False, prefetch_size=30
    )

app = App(
    driver_factory=driver,
    default_queue="samping",
    disable_cron=False,
    queue_size=0,
)
app.routes = []


@app.task(name="test_task")
async def test_task(data: str):
    logger.info("get test_task with data: %s", data)
    await asyncio.sleep(10)


@app.task(name="buggy_task")
async def buggy_task(wait: int):
    logger.info("get buggy_task with wait: %d", wait)
    await asyncio.sleep(wait)
    raise RuntimeError("buggy")


@app.tab(name="print_each_minute")
async def print_each_minute():
    logger.info("crontab print each minute executed")
    await asyncio.sleep(1)
