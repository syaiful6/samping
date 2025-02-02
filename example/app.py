import asyncio
import logging
from contextlib import asynccontextmanager

from samping.driver.sqs import SQSDriver
from samping.app import App
from samping.routes import Rule

logger = logging.getLogger("example")


shared_resource = {}

@asynccontextmanager
async def lifespan(app: App):
    logging.getLogger("samping").info("establishing connection to database")
    await asyncio.sleep(10)
    shared_resource["connection"] = "db"
    yield
    shared_resource.clear()


def driver():
    return SQSDriver(
        endpoint_url="http://localhost:9324",
        use_ssl=False,
        prefetch_size=15,
        visibility_timeout=60,
    )


app = App(
    driver_factory=driver,
    default_queue="samping",
    lifespan=lifespan,
)
app.routes = [
    Rule("test_*", "samping"),
    Rule("buggy_*", "buggy"),
]


@app.task(name="test_task")
async def test_task(data: str):
    logger.info("get test_task with data: %s", data)
    await asyncio.sleep(30)


@app.task(name="buggy_task")
async def buggy_task(wait: int):
    logger.info("get buggy_task with wait: %d", wait)
    await asyncio.sleep(wait)
    raise RuntimeError("buggy")


@app.tab(name="print_each_minute")
async def print_each_minute():
    logger.info("crontab print each minute executed")
    await asyncio.sleep(1)
