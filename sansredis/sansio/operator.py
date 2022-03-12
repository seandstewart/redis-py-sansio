from __future__ import annotations

from typing import Generic, Iterable, Union

from sansredis.sansio import events, exceptions, reader, writer
from sansredis.sansio.callbacks import errors, generic, resp2
from sansredis.sansio.types import (
    EncodableT,
    EncodedT,
    NotEnoughDataT,
    ReplyT,
    ResponseBodyT,
)


class RedisOperator(Generic[ResponseBodyT]):
    """The RedisOperator encodes and decodes data between a client and server.

    The operator is responsible for:
        1. "Packing" a client command into a byte-string the server will understand.
        2. "Un-packing" a server response into an object the client will understand.
    """

    def __init__(
        self,
        encoding: str | None = None,
        errors: str | None = None,
        notEnoughData: NotEnoughDataT = False,
    ):
        self._reader = reader.BytesReader(
            exceptions.InvalidResponse, exceptions.ResponseError, encoding, errors
        )
        self._writer = writer.Writer(encoding=encoding, encoding_errors=errors)
        self._sentinel = False
        # TODO: awaiting latest release from hiredis-py:
        #  https://github.com/redis/hiredis-py/pull/119
        if isinstance(self._reader, reader.PythonBytesReader):
            self._reader._parser.notEnoughData = notEnoughData
            self._sentinel = notEnoughData

    def pack_command(
        self, event: events.Command | events.PipelinedCommands
    ) -> events.PackedCommand:
        """Pack a command into a byte-string."""
        packed = self._writer.pack_command(event)
        return packed

    def receive_data(self, data: bytes):
        """Feed received bytes to the parser for un-packing."""
        self._reader.feed(data)

    def iterparse(self) -> Iterable:
        """Iterate over the responses un-packed from received data."""
        empty = self._sentinel
        _gets = self._reader.gets
        res = _gets()
        while res is not empty:
            yield res
            res = _gets()

    def read_response(
        self, event: events.Command | events.PipelinedCommands, response: ReplyT
    ) -> events.Response | events.PipelinedResponses | exceptions.ResponseError:
        """Read the response given by the parser and normalize the result.

        Args:
            event: The triggering command or command pipeline for this response.
            response: The parsed reply from the redis server.

        Returns:
            A normalized response object.
        """
        if isinstance(response, exceptions.ResponseError):
            return errors.parse_error(str(response))

        if isinstance(event, events.PipelinedCommands):
            return self._read_pipelined_response(event=event, reply=response)

        callback, kwargs = event.callback, event.callback_kwargs
        reply = callback(response, **kwargs) if callback else response
        return events.Response(command=event, reply=reply)

    def _read_pipelined_response(
        self,
        event: events.PipelinedCommands,
        reply: list[ReplyT] | exceptions.ResponseError,
    ) -> events.PipelinedResponses | exceptions.ResponseError:
        commands = event.commands
        truth = (event.transaction, event.raise_on_error)
        response = events.PipelinedResponses(command=commands, replies=[])
        # NOTE:
        # It's really a bit of a fallacy to combine the handling of these two responses.
        #   The fact that we (sometimes) pipeline MULTI/EXEC is an implementation
        #   detail of the client. Their responses are *fundamentally different*.
        #
        # In a "vanilla pipeline" (e.g., a multi-bulk command), the response is simply
        #   and array of replies.
        #
        # In a MULTI/EXEC pipeline, we receive:
        #   0 ->       The reply to WATCH (e.g., OK/ERR)
        #   +1 : -2 -> The result of sending each command within the transaction
        #                  (e.g., OK/ERR)
        #   -1 ->      The EXEC reply, which is an array of replies to each command.
        #                  (if the command caused an error, the reply will be missing)

        # Descending order of complexity...
        # MULTI/EXEC, raise on error
        if truth == (True, True):
            watch_error, exec_response = self._sanity_check_transaction(
                commands=commands,
                replies=reply,
            )
            if watch_error:
                return watch_error

            return self._response_or_exc(
                commands=commands, replies=reply, response=response
            )

        # MULTI/EXEC, don't raise on error
        if truth == (True, False):
            watch_error, exec_response = self._sanity_check_transaction(
                commands=commands,
                replies=reply,
            )
            replies = response.replies
            if watch_error:
                replies.append(watch_error)
            replies.extend(
                self._iter_responses(commands=commands, replies=exec_response)
            )

            return response

        # vanilla pipeline, raise the first error found
        if truth == (False, True):
            return self._response_or_exc(
                commands=commands, replies=reply, response=response
            )

        # vanilla pipeline, don't raise an error
        if truth == (False, False):
            response.replies.extend(
                self._iter_responses(commands=commands, replies=reply)
            )
            return response

    def _response_or_exc(
        self,
        *,
        commands: events.PipelinedCommands,
        replies: list[ReplyT],
        response: events.PipelinedResponses,
    ) -> events.PipelinedResponses | exceptions.PipelineResponseError:

        errs: list[tuple[int, events.Response]] = []
        errs_append = errs.append
        append = response.replies.append
        for i, resp in enumerate(
            self._iter_responses(commands=commands, replies=replies), start=1
        ):
            if isinstance(resp.reply, exceptions.ResponseError):
                errs_append((i, resp))
                continue
            append(resp)
        if errs:
            return self._annotate_exception(errors=errs)
        return response

    @staticmethod
    def _iter_responses(
        *, commands: list[events.Command], replies: list[ReplyT]
    ) -> Iterable[events.Response]:
        for cmd, reply in zip(commands, replies):
            if isinstance(reply, exceptions.ResponseError):
                yield events.Response(
                    command=cmd,
                    reply=errors.parse_error(str(reply)),
                )
                continue
            if cmd.callback:
                yield events.Response(
                    command=cmd, reply=cmd.callback(reply, **cmd.callback_kwargs)
                )
                continue
            yield events.Response(command=cmd, reply=reply)

    def _sanity_check_transaction(
        self, *, commands: list[events.Command], replies: list[ReplyT]
    ) -> tuple[exceptions.ResponseError | None, list[ReplyT]]:
        # Set up globals
        # Track if we got an error on the first watch call.
        watch_error: exceptions.ResponseError | None = None
        # Track any errors we received when sending a command within the transaction.
        exc_errors: dict[int, exceptions.ResponseError] = {}
        # Pin for faster lookups
        annoexc = self._annotate_exception
        exec_response: list[ReplyT] | None
        watch_response, exec_response = replies.pop(0), replies.pop(-1)
        # If we got a watch error, just add it to the output.
        if isinstance(watch_response, exceptions.ResponseError):
            watch_error = errors.parse_error(str(watch_response))
        # Check the command responses for an error.
        for i, (cmd, resp) in enumerate(zip(commands, replies)):
            if isinstance(resp, exceptions.ResponseError):
                exc_errors[i] = annoexc(command=cmd, reply=resp, pos=i + 1)
        # If the execution was aborted, we can't proceed.
        if isinstance(exec_response, exceptions.ResponseError):
            err = errors.parse_error(str(exec_response))
            if exc_errors:
                i, e = exc_errors.popitem()
                raise e from err
            raise err from None
        # If we received no response, a watched key was altered during the transaction.
        if exec_response is None:
            raise exceptions.WatchError("Watched variable changed.") from None

        # Add in any errors to the exec reply body
        for i, err in exc_errors.items():
            exec_response.insert(i, err)

        # Sanity check the final number of replies including errors
        if len(exec_response) != len(commands):
            raise exceptions.ResponseError(
                "Wrong number of response items from pipeline execution."
            ) from None
        return watch_error, exec_response

    @staticmethod
    def _annotate_exception(
        *, errors: list[tuple[int, events.Response]]
    ) -> exceptions.PipelineResponseError:
        annotated = [
            r.reply.__class__(
                f"Command # {i} ({generic.str_if_bytes(r.command.command)!r}) "
                f"of pipeline caused error: {str(r.reply)!r}"
            )
            for i, r in errors
        ]
        return exceptions.PipelineResponseError(
            f"Got {len(annotated):,} errors in pipeline. "
            "See the 'errors' attribute for more information.",
            errors=annotated,
        )


class RESP2RedisOperator(RedisOperator):
    def pack_command(
        self, event: events.Command | events.PipelinedCommands
    ) -> events.PackedCommand:
        event.callback = event.callback or resp2.get(event.command)
        return super().pack_command(event)


def maybe_encode(val: EncodableT) -> EncodedT:
    if isinstance(val, (bytes, bytearray, memoryview)):
        return val
    return val.encode() if isinstance(val, str) else val


_CommandT = Union[events.Command[ResponseBodyT], events.PipelinedCommands]
