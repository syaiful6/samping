from samping.driver.sqs import SQSDriver
from samping.app import App
import asyncio
import logging

logger = logging.getLogger("example")

def driver():
    return SQSDriver(
        endpoint_url="http://localhost:9324",
        use_ssl=False,
        prefetch_size=10
    )


app = App(
    driver_factory=driver,
    queue_size=50,
    default_queue="samping",
    disable_cron=True,
    worker_max_tasks=10, # set to low to easy trigger
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