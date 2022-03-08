from __future__ import annotations

from ._reader import PythonBytesReader
from .types import BytesReaderProtocol

__all__ = ("BytesReader", "BytesReaderProtocol", "PythonBytesReader")

BytesReader: BytesReaderProtocol

try:
    from hiredis import Reader as BytesReader
except (ImportError, ModuleNotFoundError):
    BytesReader = PythonBytesReader
