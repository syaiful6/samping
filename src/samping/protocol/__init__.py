import logging
import typing
from datetime import datetime

from .lifespan import LifeSpanCyle
from .queue import QueueCycle
from ..types import ASGI3Application


__all__ = [
    "LifeSpanCyle",
    "QueueCycle",
    "beat_cycle",
]

logger = logging.getLogger("samping")


async def default_receive():
    pass


async def default_send(message):
    logger.warning("asgi send called when it shouldn't")


async def beat_cycle(app: ASGI3Application, time: typing.Union[str, datetime]):
    try:
        scope = {
            "type": "beat",
            "time": time if isinstance(time, str) else time.isoformat(),
        }
        await app(scope, default_receive, default_send)
    except BaseException as exc:
        msg = "Exception in Samping application\n"
        logger.error(msg, exc_info=exc)
