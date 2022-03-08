import enum
import errno
import ssl

NONBLOCKING_EXCEPTION_ERROR_NUMBERS = {
    BlockingIOError: errno.EWOULDBLOCK,
    ssl.SSLWantReadError: 2,
    ssl.SSLWantWriteError: 2,
    ssl.SSLError: 2,
}

NONBLOCKING_EXCEPTIONS = tuple(NONBLOCKING_EXCEPTION_ERROR_NUMBERS.keys())
SYM_STAR = b"*"
SYM_DOLLAR = b"$"
SYM_CRLF = b"\r\n"
SYM_LF = b"\n"
SYM_EMPTY = b""

SERVER_CLOSED_CONNECTION_ERROR = "Connection closed by server."


class _Sentinel(enum.Enum):
    sentinel = object()


SENTINEL = _Sentinel.sentinel
MODULE_LOAD_ERROR = "Error loading the extension. Please check the server logs."
NO_SUCH_MODULE_ERROR = "Error unloading module: no such module with that name"
MODULE_UNLOAD_NOT_POSSIBLE_ERROR = "Error unloading module: operation not possible."
MODULE_EXPORTS_DATA_TYPES_ERROR = (
    "Error unloading module: the module "
    "exports one or more module-side data "
    "types, can't unload"
)
