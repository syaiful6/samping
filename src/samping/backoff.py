import asyncio
import random
from functools import wraps
from datetime import timedelta
from abc import ABC, abstractmethod

from .utils.time import utcnow


def retry(count: int = 5, on_exception=(Exception,)):
    def wrapper(fun):
        @wraps(fun)
        async def inner(*args, **kwargs):
            retries = 0
            backoff = Exponential()
            while True:
                try:
                    return await fun(*args, **kwargs)
                except on_exception as e:
                    if retries < count:
                        raise e
                    next_wait = backoff.next_backoff().total_seconds()
                    await asyncio.sleep(next_wait)
                    retries += 1

        return inner

    return wrapper


class Backoff(ABC):
    @abstractmethod
    def next_backoff() -> timedelta:
        ...

    @abstractmethod
    def reset() -> None:
        ...


class Exponential(Backoff):
    def __init__(
        self,
        initial_interval=None,
        randomization_factor=None,
        multiplier=None,
        max_interval=None,
        max_elapsed_time=None,
    ):
        self.initial_interval = initial_interval or timedelta(microseconds=500)
        self.randomization_factor = randomization_factor or 0.5
        self.multiplier = multiplier or 1.5
        self.max_interval = max_interval or 60
        self.max_elapsed_time = max_elapsed_time or timedelta(minutes=3)
        self.start_time = None
        self.current_interval = None
        self.reset()

    def reset(self):
        self.current_interval = self.initial_interval
        self.start_time = utcnow()

    def next_backoff(self):
        elapsed = self.get_elapsed_time()
        next_random = get_random_timedelta(
            self.randomization_factor, random.random(), self.current_interval
        )
        self.increment_interval()
        next_elapsed = elapsed + next_random
        if self.max_elapsed_time and next_elapsed > self.max_elapsed_time:
            return timedelta(seconds=0)
        return next_elapsed

    def get_elapsed_time(self) -> timedelta:
        return utcnow() - self.start_time

    def increment_interval(self):
        if (
            self.current_interval.total_seconds()
            >= self.max_interval // self.multiplier
        ):
            self.current_interval = timedelta(seconds=self.max_interval)
        else:
            self.current_interval = timedelta(
                seconds=self.current_interval.total_seconds() * self.multiplier
            )


def get_random_timedelta(
    randomization_factor: float, random_value: float, current_interval: timedelta
) -> timedelta:
    if randomization_factor == 0:
        return current_interval

    delta = randomization_factor * float(current_interval.total_seconds())
    min_interval = current_interval - timedelta(seconds=delta)
    max_interval = current_interval + timedelta(seconds=delta)

    duration = min_interval.total_seconds() + (
        random_value
        * ((max_interval - min_interval + timedelta(seconds=1)).total_seconds())
    )

    return timedelta(seconds=duration)
