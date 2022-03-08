from __future__ import annotations

from redis.sansio.callbacks.generic import str_if_bytes
from redis.sansio.constants import (
    MODULE_EXPORTS_DATA_TYPES_ERROR,
    MODULE_LOAD_ERROR,
    MODULE_UNLOAD_NOT_POSSIBLE_ERROR,
    NO_SUCH_MODULE_ERROR,
)
from redis.sansio.exceptions import (
    AuthenticationError,
    AuthenticationWrongNumberOfArgsError,
    BusyLoadingError,
    ExecAbortError,
    ModuleError,
    NoPermissionError,
    NoScriptError,
    ReadOnlyError,
    ResponseError,
)
from redis.sansio.types import ExceptionMappingT

__all__ = ("parse_error",)


def parse_error(response: str | bytes) -> ResponseError:
    """Parse an error response into a Python exception."""

    decoded: str = str_if_bytes(response)
    error_code, message = decoded.split(" ", maxsplit=1)
    if error_code not in EXCEPTION_CLASSES:
        return ResponseError(decoded)

    exctype_or_dict: type[ResponseError] | dict = EXCEPTION_CLASSES[error_code]
    exctype: type[ResponseError] = (
        exctype_or_dict.get(message, ResponseError)
        if isinstance(exctype_or_dict, dict)
        else exctype_or_dict
    )
    return exctype(message)


EXCEPTION_CLASSES: ExceptionMappingT = {
    "ERR": {
        "max number of clients reached": ConnectionError,
        "Client sent AUTH, but no password is set": AuthenticationError,
        "invalid password": AuthenticationError,
        # some Redis server versions report invalid command syntax
        # in lowercase
        "wrong number of arguments for 'auth' command": AuthenticationWrongNumberOfArgsError,
        # some Redis server versions report invalid command syntax
        # in uppercase
        "wrong number of arguments for 'AUTH' command": AuthenticationWrongNumberOfArgsError,
        MODULE_LOAD_ERROR: ModuleError,
        MODULE_EXPORTS_DATA_TYPES_ERROR: ModuleError,
        NO_SUCH_MODULE_ERROR: ModuleError,
        MODULE_UNLOAD_NOT_POSSIBLE_ERROR: ModuleError,
    },
    "EXECABORT": ExecAbortError,
    "LOADING": BusyLoadingError,
    "NOSCRIPT": NoScriptError,
    "READONLY": ReadOnlyError,
    "NOAUTH": AuthenticationError,
    "NOPERM": NoPermissionError,
}
