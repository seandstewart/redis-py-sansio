from __future__ import annotations

import asyncio
import queue
from typing import Callable, Literal, Mapping, Protocol, Type, TypeVar, Union

from . import exceptions
from .exceptions import InvalidResponse, ResponseError


class BytesReaderProtocol(Protocol):
    """The interface for reading Redis responses from the byte-stream.

    Follows the API defined by :py:class::`hiredis.Reader`.
    """

    def __init__(
        self,
        protocolError: ErrorHandlerT = InvalidResponse,
        replyError: ErrorHandlerT = ResponseError,
        notEnoughData: NotEnoughDataT = False,
        encoding: str | None = None,
        errors: str | None = None,
    ):
        ...

    def feed(self, data, o: int = 0, l: int = -1):  # noqa: E741
        """Feed data to parser."""
        ...

    def gets(self) -> EncodableT | NotEnoughDataT | BaseException:
        """Get parsed value or False otherwise.
        Error replies are return as replyError exceptions (not raised).
        Protocol errors are raised.
        """
        ...

    def has_data(self) -> bool:
        """Whether the"""
        ...

    def setmaxbuf(self, size: int | None) -> None:
        """No-op."""
        ...

    def getmaxbuf(self) -> int:
        """No-op."""
        ...

    def set_encoding(
        self, encoding: str | None | ... = ..., errors: str | None | ... = ...
    ):
        ...


EncodedT = Union[bytes, bytearray, memoryview]
DecodedT = Union[str, int, float]
EncodableT = Union[EncodedT, DecodedT]
EncoderT = Callable[[EncodableT], EncodedT]
ErrorHandlerT = TypeVar("ErrorHandlerT", bound=Exception)
NotEnoughDataT = TypeVar("NotEnoughDataT")
ResponseBodyT = TypeVar("ResponseBodyT")
ReplyT = Union[ResponseBodyT, exceptions.ResponseError]
ResponseHandlerT = Callable[[bytes], ResponseBodyT]
RESPVersionT = Literal["2", "3"]
QueueT = TypeVar("QueueT", asyncio.Queue, queue.Queue)
ExceptionMappingT = Mapping[str, Union[Type[Exception], Mapping[str, Type[Exception]]]]
_StringLikeT = Union[bytes, str, memoryview]
_KeyT = _StringLikeT  # Main redis key space
# Mapping is not covariant in the key type, which prevents
# Mapping[_StringLikeT, X from accepting arguments of type Dict[str, X]. Using
# a TypeVar instead of a Union allows mappings with any of the permitted types
# to be passed. Care is needed if there is more than one such mapping in a
# type signature because they will all be required to be the same key type.
AnyKeyT = TypeVar("AnyKeyT", bytes, str, memoryview)
AnyFieldT = TypeVar("AnyFieldT", bytes, str, memoryview)
KeyT = TypeVar("KeyT", bound=_KeyT)
ArgT = TypeVar("ArgT", _KeyT, EncodableT)
