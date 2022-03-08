"""Core exceptions raised by the Redis client"""
import asyncio
import builtins


class RedisError(Exception):
    pass


class ProtocolError(RedisError):
    pass


class RedisConnectionError(ConnectionError, RedisError):
    pass


class RedisTimeoutError(asyncio.TimeoutError, builtins.TimeoutError, RedisError):
    pass


class AuthenticationError(ConnectionError):
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class NoPermissionError(ResponseError):
    pass


class ModuleError(ResponseError):
    pass


class PipelineResponseError(ResponseError):
    def __init__(self, *args, errors):
        self.error = errors
        super().__init__(*args)


class LockError(RedisError, ValueError):
    """Errors acquiring or releasing a lock"""

    # NOTE: For backwards compatibility, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.
    pass


class LockNotOwnedError(LockError):
    """Error trying to extend or release a lock that is (no longer) owned"""

    pass


class ChildDeadlockedError(Exception):
    """Error indicating that a child process is deadlocked after a fork()"""

    pass


class AuthenticationWrongNumberOfArgsError(AuthenticationError):
    """
    An error to indicate that the wrong number of args
    were sent to the AUTH command
    """

    pass
