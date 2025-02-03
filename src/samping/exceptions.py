from types import TracebackType
from typing import TypeVar


BaseExceptionType = TypeVar("BaseException", bound=BaseException)


def reraise(
    tp: type[BaseExceptionType],
    value: BaseExceptionType,
    tb: TracebackType | None = None,
):
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class SampingError(BaseException):
    "Base error for Samping"


class TaskPredicate(SampingError):
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


class TaskExpectedError(TaskPredicate):
    """An error that is expected to happen every once in a while"""


class TaskTimeout(TaskPredicate):
    """Raised when a task runs over its time limit specified by the
    `time_limit` setting
    """


class TraceError(SampingError):
    """Errors that can occur while tracing a task"""


class TaskExpirationError(TraceError):
    """Raised when an expired task is received"""


class OperationalError(SampingError):
    """Recoverable message transport connection error."""


class SerializationError(SampingError):
    """Failed to serialize/deserialize content."""


class EncodeError(SerializationError):
    """Cannot encode object."""


class DecodeError(SerializationError):
    """Cannot decode object."""


class NotBoundError(SampingError):
    """Trying to call channel dependent method on unbound entity."""


class MessageStateError(SampingError):
    """The message has already been acknowledged."""


class LimitExceeded(SampingError):
    """Limit exceeded."""


class ConnectionLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous connections exceeded."""


class ChannelLimitExceeded(LimitExceeded):
    """Maximum number of simultaneous channels exceeded."""


class VersionMismatch(SampingError):
    """Library dependency version mismatch."""


class SerializerNotInstalled(SampingError):
    """Support for the requested serialization type is not installed."""


class ContentDisallowed(SerializerNotInstalled):
    """Consumer does not allow this content-type."""


class InconsistencyError(ConnectionError):
    """Data or environment has been found to be inconsistent.

    Depending on the cause it may be possible to retry the operation.
    """
