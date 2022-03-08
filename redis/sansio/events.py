from __future__ import annotations

from typing import Any, Generic

import attr

from redis.sansio.types import EncodableT, ReplyT, ResponseBodyT, ResponseHandlerT


class Event:
    __slots__ = ()


@attr.define(kw_only=True)
class Command(Event, Generic[ResponseBodyT]):
    """Represents a Redis command which client may send to the server.

    See Also:

        - [Commands](https://redis.io/commands)
    """

    __hash__ = None

    command: str | bytes
    """The top-level Redis command."""
    modifiers: list[EncodableT, ...]
    """Any arguments or modifiers for the given command."""
    callback: ResponseHandlerT[ResponseBodyT] | None
    """A callable which is run on the raw response from the Redis instance."""
    callback_kwargs: dict[str, Any]
    """Keyword arguments which will be passed to the assigned callback."""


@attr.define(kw_only=True)
class PipelinedCommands(Event):
    """A series of commands which will be executed in a single round-trip.

    See Also:

       - [Pipelining](https://redis.io/topics/pipelining)
       - [Transactions](https://redis.io/topics/transactions)
    """

    __hash__ = None

    commands: list[Command]
    """The series of commands to send to the Redis server."""
    transaction: bool = False
    """Whether to run these commands under a MULTI/EXEC transaction."""
    raise_on_error: bool = False
    """Whether to raise any received errors, or just return them."""


@attr.define(kw_only=True)
class PackedCommand(Event):
    """Represents an encoded command which will be sent to the Redis server.

    See Also:

        - [Pipelining](https://redis.io/topics/pipelining)
        - [Protocol](https://redis.io/topics/protocol)
        - [Commands](https://redis.io/commands)
    """

    __hash__ = None

    command: Command | PipelinedCommands
    """The originating un-encoded command or command pipeline."""
    payload: bytearray
    """The command encoded into a binary string for sending to the server."""


@attr.define(kw_only=True)
class PackedResponse(Event):
    """Represents an un-parsed response from the Redis server.

    See Also:

        - [Protocol](https://redis.io/topics/protocol)
    """

    __hash__ = None

    command: Command | PipelinedCommands
    """The originating un-encoded command or command pipeline."""
    payload: bytes
    """The RESP-encoded byte-string response from the server."""


@attr.define(kw_only=True)
class Response(Event, Generic[ResponseBodyT]):
    """Represents a parsed response from the redis server."""

    __hash__ = None

    command: Command[ResponseBodyT] | None
    """The originating un-encoded command or command pipeline."""
    reply: ReplyT[ResponseBodyT]
    """The parsed response from the server, including all client-provided callbacks."""


@attr.define(kw_only=True)
class PipelinedResponses(Event):
    """Represents a series of parsed responses to a pipelined command."""

    __hash__ = None

    commands: PipelinedCommands
    replies: list[ReplyT]


@attr.define(kw_only=True)
class ConnectionClosed(Event):
    ...
