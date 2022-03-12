from __future__ import annotations

from ._parser import PythonParser
from .exceptions import InvalidResponse, ResponseError
from .types import BytesReaderProtocol, EncodableT, ErrorHandlerT, NotEnoughDataT

__all__ = ("PythonBytesReader",)


class PythonBytesReader(BytesReaderProtocol):
    """Pure-Python RESP2/3 parser following :py:class:`hiredis.Reader` interface."""

    __slots__ = ("_parser",)

    def __init__(
        self,
        protocolError: ErrorHandlerT = InvalidResponse,
        replyError: ErrorHandlerT = ResponseError,
        notEnoughData: NotEnoughDataT = False,
        encoding: str | None = None,
        errors: str | None = None,
    ):
        self._parser = PythonParser(
            protocolError=protocolError,
            replyError=replyError,
            notEnoughData=notEnoughData,
            encoding=encoding,
            errors=errors,
        )

    def feed(self, data, o: int = 0, l: int = -1):  # noqa: E741
        """Feed data to parser."""
        if l == -1:  # noqa: E741
            l = len(data) - o  # noqa: E741
        if o < 0 or l < 0:
            raise ValueError("negative input")
        if o + l > len(data):
            raise ValueError("input is larger than buffer size")
        self._parser.buf.extend(data[o : o + l])

    def gets(self) -> EncodableT | NotEnoughDataT | BaseException:
        """Get parsed value or False otherwise.
        Error replies are return as replyError exceptions (not raised).
        Protocol errors are raised.
        """
        return self._parser.parse_one()

    def has_data(self) -> bool:
        """Whether the buffer has data pending read."""
        return len(self._parser.buf) > self._parser.pos

    def setmaxbuf(self, size: int | None) -> None:
        """No-op."""
        pass

    def getmaxbuf(self) -> int:
        """No-op."""
        return 0

    def set_encoding(
        self, encoding: str | None | ... = ..., errors: str | None | ... = ...
    ):
        if encoding is not ...:
            self._parser.encoding = encoding
        if errors is not ...:
            self._parser.encoding_errors = errors or "strict"
