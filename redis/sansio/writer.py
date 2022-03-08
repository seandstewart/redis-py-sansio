from __future__ import annotations

from itertools import chain

from redis.sansio import events, types
from redis.sansio.exceptions import DataError


class Writer:
    """A Sans-IO 'Writer', which will encode the given command into bytes.

    The encoded bytes will follow the Redis Multi-bulk protocol.
    """
    __slots__ = ("encoding", "encoding_errors")

    def __init__(self, *, encoding: str | None = None, encoding_errors: str | None = None):
        self.encoding = encoding
        self.encoding_errors = encoding_errors
        self._converters[str] = self._get_str_encoder()

    def _get_str_encoder(self):
        if self.encoding:
            if self.encoding_errors:
                def str_encoder_errors(val: str, *, __encoding=self.encoding, __errors=self.encoding_errors):
                    return val.encode(val, __encoding, __errors)
                return str_encoder_errors

            def str_encoder_encoding(val: str, *, __encoding=self.encoding):
                return val.encode(__encoding)
            return str_encoder_encoding

        return lambda val: val.encode()

    def encode(self, val: types.EncodableT) -> types.EncodedT:
        cls = val.__class__
        if cls not in self._valid:
            raise DataError(
                f"Invalid type given: {cls.__name__!r}. "
                f"Convert to one of {(*(t.__name__ for t in self._valid),)} first."
            )
        _conv = self._converters
        return _conv[cls](val) if cls in _conv else val

    def pack_command(
        self, event: events.Command | events.PipelinedCommands
    ) -> events.PackedCommand:
        """Pack a command into a bytearray to send to the downstream peer,"""
        payload = (
            self._pack_command(event)
            if isinstance(event, events.Command)
            else self._pack_pipeline(event)
        )
        return events.PackedCommand(command=event, payload=payload)

    def _pack_pipeline(self, event: events.PipelinedCommands) -> bytearray:
        output: bytearray = bytearray()
        for cmd in event.commands:
            output = self._pack_command(cmd)
        return output

    def _pack_command(
        self, event: events.Command, *, buf: bytearray = None
    ) -> bytearray:
        buf = bytearray() if buf is None else buf
        cmd = event.command.split()
        args = event.modifiers
        buf.extend(b"*%d\r\n" % (len(cmd) + len(args)))
        _extend = buf.extend
        _encode = self.encode
        for arg in chain(cmd, args):
            barg = _encode(arg)
            _extend(b"$%d\r\n%s\r\n" % (len(barg), barg))

        return buf

    _valid = frozenset((bytes, bytearray, memoryview, str, int, float))
    _converters = {
        str: lambda val: val.encode(),
        int: lambda val: b"%d" % val,
        float: lambda val: b"%r" % val,
    }
