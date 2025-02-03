import asyncio
import typing
import logging

from ..types import ASGI3Application

logger = logging.getLogger("samping")


STATE_TRANSITION_ERROR = "Got invalid state transition on lifespan protocol."


class LifeSpanCyle:
    def __init__(self, app: ASGI3Application):
        self.startup_event = asyncio.Event()
        self.shutdown_event = asyncio.Event()
        self.receive_queue = asyncio.Queue()
        self.error_occured = False
        self.startup_failed = False
        self.shutdown_failed = False
        self.should_exit = False
        self.app = app
        self.state: dict[str, typing.Any] = {}

    async def __aenter__(self):
        logger.info("Waiting for application startup")
        loop = asyncio.get_event_loop()
        main_lifespan = loop.create_task(self.main())

        startup_event = {"type": "lifespan.startup"}
        await self.receive_queue.put(startup_event)
        await self.startup_event.wait()

        if self.startup_failed or self.error_occured:
            logger.error("Application startup failed. Exiting.")
            self.should_exit = True
        else:
            logger.info("Application startup completed.")

        return self

    async def __aexit__(self, exc_type, exc_value, tb):
        if self.error_occured:
            return
        logger.info("Waiting for application shutdown.")
        shutdown_event = {"type": "lifespan.shutdown"}
        await self.receive_queue.put(shutdown_event)
        await self.shutdown_event.wait()

        if self.shutdown_failed or self.error_occured:
            logger.error("Application shutdown failed. Exiting.")
            self.should_exit = True
        else:
            logger.info("Application shutdown complete")

    async def main(self):
        try:
            scope = {
                "type": "lifespan",
                "asgi": {"version": "2.0", "spec_version": "2.0"},
                "state": self.state,
            }
            await self.app(scope, self.receive, self.send)
        except BaseException as exc:
            self.error_occured = True
            if self.startup_failed or self.shutdown_failed:
                return
            else:
                msg = "Exception in 'lifespan' protocol\n"
                logger.error(msg, exc_info=exc)
        finally:
            self.startup_event.set()
            self.shutdown_event.set()

    async def send(self, message: dict):
        assert message["type"] in (
            "lifespan.startup.complete",
            "lifespan.startup.failed",
            "lifespan.shutdown.complete",
            "lifespan.shutdown.failed",
        )

        if message["type"] == "lifespan.startup.complete":
            assert not self.startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not self.shutdown_event.is_set(), STATE_TRANSITION_ERROR
            self.startup_event.set()

        elif message["type"] == "lifespan.startup.failed":
            assert not self.startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not self.shutdown_event.is_set(), STATE_TRANSITION_ERROR
            self.startup_event.set()
            self.startup_failed = True
            if message.get("message"):
                logger.error(message["message"])

        elif message["type"] == "lifespan.shutdown.complete":
            assert self.startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not self.shutdown_event.is_set(), STATE_TRANSITION_ERROR
            self.shutdown_event.set()

        elif message["type"] == "lifespan.shutdown.failed":
            assert self.startup_event.is_set(), STATE_TRANSITION_ERROR
            assert not self.shutdown_event.is_set(), STATE_TRANSITION_ERROR
            self.shutdown_event.set()
            self.shutdown_failed = True
            if message.get("message"):
                logger.error(message["message"])

    async def receive(self):
        return await self.receive_queue.get()
