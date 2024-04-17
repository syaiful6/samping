class TaskPredicate(Exception):
    """Base class for task-related semi-predicates."""


class Retry(TaskPredicate):
    #: Optional message
    message = None

    #: Exception (if any) that caused the retry to happen.
    exc = None

    #: Time of retry (ETA) in seconds
    when = None

    def __init__(self, exc=None, when=None, **kwargs):
        self.when = when
        super().__init__(self, exc, when, **kwargs)


class WorkerStopSignal(Exception):
    pass
