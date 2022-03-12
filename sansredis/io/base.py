from __future__ import annotations

import collections
from typing import Any, Deque, Generic, Iterator, Mapping, NoReturn, Protocol, TypeVar

from sansredis.sansio import constants, events
from sansredis.sansio import protocol as proto
from sansredis.sansio import types

_CT = TypeVar("_CT")
_RT = TypeVar("_RT")


class BaseIORedisConnectionPool(Generic[_CT, _RT]):
    """A blocking connection pool implementation for sync-io.

    This implementation is optimized maintaining a pool of ready-to-use connections
    under high load while keeping the open connections to a minimum desired amount.

    Throughput can be effectively tuned via `min_connections` and `max_connections`.

    Notes:
        When entering the pool context (or awaiting the pool object), the pool is
        pre-filled with `min_connections` of active connections to Redis.

        When those connections are exhausted, the pool will begin checking out new
        connections, until `max_connections` is reached, at which point the pool will
        wait for an in-use  connection to be released back into the pool before yielding
        a connection to the waiter.

        This can mean some tasks may block on other active tasks, but in testing
        it's been found to be much more resilient under heavy load than a locking-based
        approach.
    """

    __slots__ = (
        "address_info",
        "socket_info",
        "client_info",
        "pool_info",
        "ssl_info",
        "protocol",
        "free",
        "inuse",
        "acquiring",
        "_connection_waiter",
    )

    def __init__(
        self,
        *,
        protocol: proto.SansIORedisProtocol = None,
        address_info: proto.AddressInfo = None,
        socket_info: proto.SocketInfo = None,
        client_info: proto.ClientInfo = None,
        pool_info: proto.PoolInfo = None,
        ssl_info: proto.SSLInfo = None,
        use_ssl: bool = False,
    ):
        self.protocol = protocol or proto.SansIORedisProtocol(
            address_info=address_info,
            client_info=client_info,
            socket_info=socket_info,
            pool_info=pool_info,
            ssl_info=ssl_info,
            use_ssl=use_ssl,
        )
        self.address_info = protocol.address_info
        self.socket_info = protocol.socket_info
        self.client_info = protocol.client_info
        self.pool_info = protocol.pool_info
        self.ssl_info = protocol.ssl_info
        self.free: Deque[_CT] = collections.deque(maxlen=self.pool_info.max_connections)
        self.inuse: set[_CT] = set()
        self.acquiring: int = 0
        self._connection_waiter = self._get_waiter()

    def size(self) -> int:
        return len(self.free) + len(self.inuse) + self.acquiring

    def available(self) -> int:
        return len(self.free)

    def execute_command(
        self,
        command: str | bytes,
        *args,
        callback: types.ResponseHandlerT[types.ResponseBodyT] = None,
        **callback_kwargs,
    ) -> types.ResponseBodyT:
        """Get a free connection from the pool and execute the given command.

        Args:
            command: The Redis command to execute on the server.
            *args: Any arguments or modifiers for the given command.
            callback: A callback for the response from the server.
            **callback_kwargs: Any keyword arguments to pass on to the callback.

        Returns:
            A response from the Redis server.
        Raises:
            A :py:class:`~redis.sansio.exceptions.RedisError`.
        """
        conn = self._get_conn()
        if conn:
            return conn.execute_command(
                command,
                *args,
                callback=callback,
                **callback_kwargs,
            )
        return self._wait_execute_command(
            command, *args, callback=callback, **callback_kwargs
        )

    def execute_pipeline(self, event: events.PipelinedCommands):
        """Send a multi-bulk pipeline of commands to the server in one round-trip.

        Args:
            event: The pipelined commands to send to Redis.

        Returns:
            An array of responses (and errors, if `event.raise_on_error` is `False`).

        Raises:
            A :py:class:`~redis.sansio.exceptions.RedisError`.
        """
        conn = self._get_conn()
        if conn:
            return conn.execute_pipeline(event)
        return self._wait_execute_pipeline(event)

    def pipeline(
        self,
        *commands: events.Command,
        transaction: bool = False,
        raise_on_error: bool = False,
    ) -> events.PipelinedCommands:
        """Create a pipeline for packing multiple commands into a single rount-trip.

        Args:
            *commands: any pre-made commands you wish to execute within this pipeline.
            transaction: Whether to execute all commands under a single MULTI/EXEC.
            raise_on_error: Whether to raise the first error received, of return it.
        """
        return self.protocol.make_pipeline(
            commands=[*commands], transaction=transaction, raise_on_error=raise_on_error
        )

    def extend_pipeline(
        self,
        command: str,
        *args,
        pipeline: events.PipelinedCommands,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ) -> NoReturn:
        """Add a command to the current multi-bulk pipeline.

        Args:
            command: The Redis command to execute on the server.
            *args: Any arguments or modifiers for the given command.
            pipeline: The command pipeline to add this command to.
            callback: A callback for the response from the server.
            **callback_kwargs: Any keyword arguments to pass on to the callback.
        """
        self.protocol.extend_pipeline(
            command=command,
            *args,
            pipeline=pipeline,
            callback=callback,
            **callback_kwargs,
        )

    def iterconn(self, override_min: bool) -> Iterator[_CT]:
        maxc = self.pool_info.max_connections
        minc = self.pool_info.min_connections
        self._drop_closed()
        while self.size() < minc:
            # Fill the min amount with active connections.
            self.acquiring += 1
            try:
                c = self.make_connection()
                yield c

            finally:
                self.acquiring -= 1
            self._drop_closed()

        if override_min:
            while self.size() < maxc and not self.available():
                # Fill the min amount with active connections.
                self.acquiring += 1
                try:
                    c = self.make_connection()
                    yield c

                finally:
                    self.acquiring -= 1
                self._drop_closed()

    def _drop_closed(self):
        for _ in range(self.available()):
            conn = self.free[0]
            if not conn.is_connected:
                self.free.popleft()
            else:
                self.free.rotate(1)

    def connection(self):
        """Check out a new connection from the pool within a context manager."""
        raise NotImplementedError()

    def acquire(self):
        """Manually acquire a connection from the pool.

        If `max_connections` has been reached, this method will block until a
        connection is released back to the pool.
        """
        raise NotImplementedError()

    def release(self, connection: _CT):
        """Release a connection back into the pool.

        If we don't own this connection, it will be disconnected and discarded.

        Args:
            connection: The checked-out connection.
        """
        raise NotImplementedError()

    def fill(self, *, override_min: bool = False):
        """Fill the pool to at least the min connection count.

        If `override_min` is `True`, keep adding active connections until there
        is a free one in the pool, until we hit the `max_connections` value.

        Args:
            override_min: Whether to keep adding connections after we've hit the minimum.
        """
        raise NotImplementedError()

    def disconnect(self, *, inuse: bool = False):
        """Disconnect all free connections in the pool.

        If `inuse` is `True`, then also shutdown all checked out connections.

        Args:
            inuse: Whether we should close all checked out connections as well.
        """
        raise NotImplementedError()

    def reset(self, *, inuse: bool = False):
        """Discard all currnt connections and fill the pool with new ones.

        Args:
            inuse: Whether we should close all checked out connections as well.
        """
        raise NotImplementedError()

    def make_connection(self) -> _CT:
        """A connection factory."""
        raise NotImplementedError()

    def _wait_execute_command(
        self,
        command: str,
        *args,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ) -> _RT:
        raise NotImplementedError()

    def _wait_execute_pipeline(self, event: events.PipelinedCommands):
        raise NotImplementedError()

    def _get_conn(self) -> _CT | None:
        # Get a connection, fast and dirty. Do not use in public API.
        #   We can only do this if there are currently free connections.
        if not self.free:
            return
        for _ in range(self.available()):
            conn = self.free[0]
            # Rotate the pool so that we don't overload this connection.
            self.free.rotate(1)
            if conn.is_connected:
                return conn

    def _wakeup(self):
        raise NotImplementedError()

    def _get_waiter(self):
        raise NotImplementedError()


class BaseIORedisConnection(Generic[_RT]):
    __slots__ = (
        "protocol",
        "connection",
        "connect_routine",
        "_connectlock",
        "_ioprotocol",
    )
    _ioprotocol: IOProtocolProtocol

    def __init__(self, *, protocol: proto.SansIORedisProtocol = None):
        self.protocol = protocol or proto.SansIORedisProtocol()
        connect_routine = None
        if self.protocol.client_info.server_version:
            connect_routine = self.protocol.get_on_connect_routine()
        self.connect_routine: proto.OnConnectRoutineT | None = connect_routine

    @property
    def is_connected(self):
        return self.connection and self._ioprotocol.is_connected

    def execute_command(
        self,
        command: str,
        *args,
        callback: types.ResponseHandlerT[types.ResponseBodyT] = None,
        **callback_kwargs,
    ) -> _RT:
        """Send a command to the Redis server and parse the response.

        Args:
            command: The Redis command to execute on the server.
            *args: Any arguments or modifiers for the given command.
            callback: A callback for the response from the server.
            **callback_kwargs: Any keyword arguments to pass on to the callback.

        Returns:
            A response from the Redis server.
        Raises:
            A :py:class:`~redis.sansio.exceptions.RedisError`.
        """
        event = self.protocol.make_command(
            command,
            *args,
            callback=callback,
            **callback_kwargs,
        )
        packed = self.protocol.pack_command(event)
        return self._do_send_and_read_command(packed)

    def execute_pipeline(self, event: events.PipelinedCommands):
        """Send a multi-bulk pipeline of commands to the server in one round-trip.

        Args:
            event: The pipelined commands to send to Redis.

        Returns:
            An array of responses (and errors, if `event.raise_on_error` is `False`).

        Raises:
            A :py:class:`~redis.sansio.exceptions.RedisError`.
        """
        packed = self.protocol.pack_command(event)
        return self._do_send_and_read_pipeline(packed)

    def pipeline(
        self, transaction: bool = False, raise_on_error: bool = False
    ) -> events.PipelinedCommands:
        """Create a pipeline for packing multiple commands into a single rount-trip.

        Args:
            transaction: Whether to execute all commands under a single MULTI/EXEC.
            raise_on_error: Whether to raise the first error received, of return it.
        """
        return self.protocol.make_pipeline(
            commands=[], transaction=transaction, raise_on_error=raise_on_error
        )

    def extend_pipeline(
        self,
        command: str,
        *args,
        pipeline: events.PipelinedCommands,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ) -> NoReturn:
        """Add a command to the current multi-bulk pipeline.

        Args:
            command: The Redis command to execute on the server.
            *args: Any arguments or modifiers for the given command.
            pipeline: The command pipeline to add this command to.
            callback: A callback for the response from the server.
            **callback_kwargs: Any keyword arguments to pass on to the callback.
        """
        self.protocol.extend_pipeline(
            command=command,
            *args,
            pipeline=pipeline,
            callback=callback,
            **callback_kwargs,
        )

    def send_command(self, event: events.PackedCommand):
        """Send a packed command over the wire.

        Args:
            event: The encoded redis command, associated to the client command.

        Returns:
            A :py::class:`concurrent.futures.Future` object.
        """
        return self._ioprotocol.send_command(event)

    def connect(self):
        raise NotImplementedError()

    def on_connect(self):
        raise NotImplementedError()

    def disconnect(self):
        raise NotImplementedError()

    def check_health(self):
        raise NotImplementedError()

    def read_response(self, future) -> _RT:
        """Wait for the response from the server.

        Args:
            future: A Future-like object attached to the server response.

        Returns:
            A response event with parsed reply or replies.

        Raises:
            :py::class:`~redis.sansio.exceptions.RedisTimeoutError`
            :py::class:`~redis.sansio.exceptions.InvalidResponse`
            :py::class:`~redis.sansio.exceptions.ResponseError`
        """
        raise NotImplementedError()

    def _do_send_and_read_command(self, event: events.PackedCommand) -> _RT:
        raise NotImplementedError()

    def _do_send_and_read_pipeline(self, event: events.PackedCommand) -> _RT:
        raise NotImplementedError()


class IOProtocolProtocol(Protocol):
    @property
    def is_connected(self) -> bool:
        return False

    def wait_connected(self):
        ...

    def wait_disconnected(self):
        ...

    def send_command(self, event: events.PackedCommand):
        ...

    def connection_made(self, transport):
        ...

    def connection_lost(self, exc: BaseException | None):
        ...
