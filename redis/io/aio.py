from __future__ import annotations

import asyncio
import collections
import enum
import socket
from typing import Awaitable

import async_timeout

from redis.io import base
from redis.io.base import _RT
from redis.sansio import events, exceptions, protocol, types
from redis.sansio.callbacks.resp2 import meta


class AsyncIORedisConnectionPool(
    base.BaseIORedisConnectionPool[
        "AsyncIORedisConnection", Awaitable[types.ResponseBodyT]
    ]
):
    """A blocking connection pool implementation for async-io.

    This implementation is optimized maintaining a pool of ready-to-use connections
    under high load while keeping the open connections to a minimum desired amount.

    Throughput can be effectively tuned via `min_connections` and `max_connections`.

    Notes:
        When entering the pool context (or awaiting the pool object), the pool is
        pre-filled with `min_connections` of active connections to Redis. When those
        connections are exhausted, the pool will begin checking out new connections, until
        `max_connections` is reached, at which point the pool will wait for an in-use
        connection to be released back into the pool before yielding a connection to the
        waiter. This can mean some tasks may block other active tasks, but in testing
        it's been found to be much more resilient under heavy load than a locking-based
        approach.
    """

    def _get_waiter(self):
        return asyncio.Condition()

    def __await__(self):
        return self.fill().__await__()

    async def __aenter__(self):
        await self
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect(inuse=True)

    async def _wait_execute_command(
        self,
        command: str,
        *args,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ):
        async with self.connection() as conn:
            return await conn.execute_command(
                command,
                *args,
                callback=callback,
                **callback_kwargs,
            )

    async def _wait_execute_pipeline(self, event: events.PipelinedCommands):
        async with self.connection() as conn:
            return await conn.execute_pipeline(event=event)

    def connection(self) -> _AsyncIOPoolConnectionContext:
        """Check out a new connection from the pool.

        May be used as an async context-manager or directly `await`-ed.
        """
        return _AsyncIOPoolConnectionContext(pool=self)

    async def acquire(self) -> AsyncIORedisConnection:
        """Manually acquire a connection from the pool.

        If `max_connections` has been reached, this method will block until a
        connection is released back to the pool.
        """
        async with self._connection_waiter:
            while True:
                # Add at least one connection to the pool, if possible.
                await self.fill(override_min=True)
                # If we have available connection(s), grab one.
                if self.available():
                    conn = self.free.popleft()
                    self.inuse.add(conn)
                    return conn
                # Otherwise, wait until a connection is released.
                else:
                    await self._connection_waiter.wait()

    async def _wakeup(self):
        # Notify any connection waiters that they can check for a connection.
        async with self._connection_waiter:
            self._connection_waiter.notify()

    async def release(self, connection: AsyncIORedisConnection):
        """Release a connection back into the pool.

        If we don't own this connection, it will be disconnected and discarded.

        Args:
            connection: The checked-out connection.
        """
        if not connection:
            return
        if connection not in self.inuse:
            await connection.disconnect()
            return
        self.inuse.remove(connection)
        if connection.is_connected:
            self.free.append(connection)
        asyncio.ensure_future(self._wakeup())

    async def fill(self, *, override_min: bool = False):
        """Fill the pool to at least the min connection count.

        If `override_min` is `True`, keep adding active connections until there
        is a free one in the pool, until we hit the `max_connections` value.

        Args:
            override_min: Whether to keep adding connections after we've hit the minimum.
        """
        for conn in self.iterconn(override_min):
            await conn.connect()
            self.free.append(conn)

        asyncio.ensure_future(self._wakeup())

    async def disconnect(self, *, inuse: bool = False):
        """Disconnect all free connections in the pool.

        If `inuse` is `True`, then also shutdown all checked out connections.

        Args:
            inuse: Whether we should close all checked out connections as well.
        """
        async with self._connection_waiter:
            tasks = []
            while self.free:
                conn: AsyncIORedisConnection = self.free.pop()
                tasks.append(asyncio.create_task(conn.disconnect()))
            while inuse and self.inuse:
                conn: AsyncIORedisConnection = self.inuse.pop()
                tasks.append(asyncio.create_task(conn.disconnect()))
            resp = await asyncio.gather(*tasks, return_exceptions=True)
            exc = next((r for r in resp if isinstance(r, BaseException)), None)
            if exc:
                raise exc

    async def reset(self, *, inuse: bool = False):
        """Discard all currnt connections and fill the pool with new ones.

        Args:
            inuse: Whether we should close all checked out connections as well.
        """
        await self.disconnect(inuse=inuse)
        await self.fill()

    def make_connection(self) -> AsyncIORedisConnection:
        """A connection factory."""
        return AsyncIORedisConnection(protocol=self.protocol)


class AsyncIORedisConnection(
    base.BaseIORedisConnection[Awaitable[types.ResponseBodyT]]
):
    """A Redis-compliant connection object.

    The `AsyncIORedisConnection` manages an open `asyncio.Transport` and an
    `asyncio.Protocol` subclass which uses the `RedisProtocol` class to manage the
    request<->response lifecycle for a Redis Client and Server.
    """

    def __init__(self, *, protocol: protocol.SansIORedisProtocol = None):
        super().__init__(
            protocol=protocol,
        )
        self.connection: asyncio.Transport | None = None
        self._connectlock = asyncio.Lock()
        self._responses = asyncio.Queue()
        self._ioprotocol = RedisAsyncIOProtocol(self.protocol)

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect()

    async def connect(self):
        """Connects to the Redis server if not already connected"""
        if self.is_connected:
            return

        async with self._connectlock:
            if self.is_connected:
                return
            try:
                await self._connect()
            except asyncio.CancelledError:
                raise
            except (socket.timeout, asyncio.TimeoutError):
                raise TimeoutError("Timeout connecting to server")
            except OSError as e:
                raise self.protocol.connection_error(e) from None
            except Exception as exc:
                raise self.protocol.connection_error(exc) from exc

            try:
                await self.on_connect()
            except exceptions.RedisError:
                # clean up after any error in on_connect
                await self.disconnect()
                raise

    async def _connect(self):
        is_unix = self.protocol.socket_info.is_unix_socket
        timeout = self.protocol.socket_info.connect_timeout
        address = self.protocol.address_info
        ssl_info = self.protocol.ssl_info
        ssl_context = ssl_info and ssl_info.get_context()
        async with async_timeout.timeout(timeout):
            loop = asyncio.get_running_loop()
            if is_unix:
                self.connection, _ = await loop.create_unix_connection(
                    lambda: self._ioprotocol, path=address.host, ssl=ssl_context
                )
            else:
                self.connection, _ = await loop.create_connection(
                    lambda: self._ioprotocol,
                    host=address.host,
                    port=address.port,
                    ssl=ssl_context,
                )
            await self._ioprotocol.wait_connected()

    async def on_connect(self):
        """Initialize the operator, authenticate and select a database"""
        version = self.protocol.client_info.server_version
        routine = self.connect_routine
        if version is None:
            info = await self.execute_command(
                "INFO", "server", callback=meta.parse_info
            )
            version_str = info["redis_version"]
            self.protocol.client_info.server_version = protocol.ServerVersion(
                *(int(v) for v in version_str.split("."))
            )

        if routine is None:
            self.connect_routine = routine = self.protocol.get_on_connect_routine()
        init, stack = routine
        # This must happen first to enable further interactions with the server.
        if init:
            try:
                iresponse = await self.send_command(event=init)
                return iresponse
            except exceptions.AuthenticationError as e:
                raise e from None
            except exceptions.ResponseError as e:
                raise self.protocol.connection_error(e) from e
        if stack:
            try:
                await self.send_command(event=stack)
            except exceptions.ResponseError as e:
                raise self.protocol.connection_error(e) from e

    async def disconnect(self):
        """Disconnects from the Redis server"""
        timeout = self.protocol.socket_info.connect_timeout
        try:
            async with async_timeout.timeout(timeout):
                if not self.is_connected:
                    return
                try:
                    self.connection.close()
                    await self._ioprotocol.wait_disconnected()
                    self.connection = None
                except OSError:
                    pass
        except asyncio.TimeoutError:
            raise TimeoutError(
                f"Timed out closing connection after {timeout}."
            ) from None

    async def check_health(self):
        """Check the health of the operator with a PING/PONG"""
        loop = asyncio.get_running_loop()
        if self.protocol.should_check_health(loop.time()):
            command = self.protocol.get_health_check()
            try:
                response = await self.send_command(command)
                self.protocol.check_health_response(response)
            except (ConnectionError, TimeoutError) as err:
                await self.disconnect()
                try:
                    response = await self.send_command(command)
                    self.protocol.check_health_response(response)
                except BaseException as err2:
                    raise err2 from err

            self.protocol.set_next_health_check(loop.time())

    async def read_response(
        self, future: asyncio.Future
    ) -> events.Response | events.PipelinedResponses:
        """Wait for the response from the server.

        Args:
            future: An :py::class:`asyncio.Future` attached to the server response.

        Returns:
            A response event with parsed reply or replies.

        Raises:
            :py::class:`~redis.sansio.exceptions.RedisTimeoutError`
            :py::class:`~redis.sansio.exceptions.InvalidResponse`
            :py::class:`~redis.sansio.exceptions.ResponseError`
        """
        try:
            async with async_timeout.timeout(self.protocol.socket_info.timeout):
                response = await future
        except asyncio.TimeoutError:
            raise exceptions.RedisTimeoutError("Timed out waiting for response.")
        if isinstance(response, Exception):
            raise response from None
        return response

    async def _do_send_and_read_command(self, event: events.PackedCommand) -> _RT:
        fut = self.send_command(event)
        response = await self.read_response(fut)
        return response.reply

    async def _do_send_and_read_pipeline(self, event: events.PackedCommand) -> _RT:
        fut = self.send_command(event)
        response = await self.read_response(fut)
        return response.replies


class RedisAsyncIOProtocol(asyncio.Protocol):
    """A :py::class:`asyncio.Protocol` for reading and writing Redis commands.

    The RedisAsyncIOProtocol is fed directly to the :py::class:`asyncio.Transport`
    on connection creation. The event loop will call each method which aligns with
    pre-defined loop events.

    The custom `send_command` method is implemented to allow us to track the
    request<->response lifecycle by attaching it to an :py::class:`asyncio.Future`
    which the client can wait for.
    """

    __slots__ = (
        "operator",
        "proto",
        "_state",
        "_waiters",
        "_transport",
        "_exc",
        "_conn_waiter",
        "_disconnect_waiter",
    )

    def __init__(
        self,
        proto: protocol.SansIORedisProtocol,
    ):
        self.operator = proto.operator
        self.proto = proto
        self._state = _State.not_connected
        self._waiters = collections.deque()
        self._transport: asyncio.Transport | None = None
        self._exc: BaseException | None = None
        self._conn_waiter: asyncio.Event = asyncio.Event()
        self._disconnect_waiter: asyncio.Event = asyncio.Event()

    @property
    def is_connected(self):
        return self._state == _State.connected and not self._transport.is_closing()

    def connection_made(self, transport: asyncio.Transport) -> None:
        self._transport = transport
        sock = transport.get_extra_info("socket")
        if sock is not None:
            self.proto.configure_socket(sock, settimeout=False)

        self._state = _State.connected
        self._conn_waiter.set()

    @property
    def connected(self) -> bool:
        return self._state == _State.connected

    async def wait_connected(self) -> None:
        """Wait to access the operator until `connection_made` is complete."""
        await self._conn_waiter.wait()

    async def wait_disconnected(self):
        await self._disconnect_waiter.wait()

    def send_command(self, event: events.PackedCommand) -> asyncio.Future:
        if self._state == _State.connected:
            # It's possible the connection was dropped and connection_lost was not
            # called yet. To stop spamming errors, avoid writing to broken pipe
            # Both _UnixWritePipeTransport and _SelectorSocketTransport that we
            # expect to see here have this attribute
            fut = asyncio.get_running_loop().create_future()
            if self._transport.is_closing():
                fut.set_result(events.ConnectionClosed())
                return fut

            # Write the packed byte-stream to the socket.
            self._transport.write(event.payload)
            # Add this command and the associated future to our stack of pending responses.
            self._waiters.append((event.command, fut))
            # Return the future so the caller can await the result.
            return fut

        elif self._state == _State.not_connected:
            raise ConnectionError(
                f"Lost operator while sending command: {event.command!r}"
            )

        elif self._state == _State.error:
            exc = self._exc
            if exc is None:
                exc = ConnectionError(
                    f"Got an unknown error while sending command: {event.command}."
                )
            raise exc

    def data_received(self, data: bytes) -> None:
        """Send the received data to be parsed.

        Once the data is parsed, associate it back with the triggering command.
        """
        if self._state != _State.connected:
            return

        self.operator.receive_data(data)
        _get_fut = self._get_fut
        for parsed in self.operator.iterparse():
            item = _get_fut()
            # If there is no pending response, we should just move on.
            if item is None:
                continue
            cmd, fut = item
            # Parse the reply and run it through any callbacks.
            response = self.operator.read_response(cmd, parsed)
            # Bubble up the exception if that's the result of the parse.
            if isinstance(response, Exception):
                fut.set_exception(response)
            # Otherwise, set the result so the waiter can receive it.
            else:
                fut.set_result(response)

    def _get_fut(self) -> tuple[events.Command, asyncio.Future] | None:
        try:
            item = self._waiters.popleft()
            return item
        except IndexError:
            # Extra unexpected data received from operator
            # e.g. connected to non-redis service
            err = exceptions.InvalidResponse(
                "Got additional data on the stream. "
                "Are you connected to a supported Redis instance?"
            )
            self._set_exception(err)

    def connection_lost(self, exc: BaseException | None):
        """Called when a connection is closing to prevent further operations."""
        if exc is not None:
            self._set_exception(exc)

        elif self._state == _State.connected:
            exc = ConnectionError("Lost connection to server.")
            self._state = _State.not_connected
            self._set_exception(exc)
        self._disconnect_waiter.set()

    def _set_exception(self, exc):
        self._exc = exc
        self._state = _State.error


class _AsyncIOPoolConnectionContext:
    __slots__ = ("pool", "conn")

    def __init__(self, pool: AsyncIORedisConnectionPool):
        self.pool = pool
        self.conn = None

    def __await__(self):
        return self.pool.acquire().__await__()

    async def __aenter__(self):
        self.conn = await self.pool.acquire()
        return self.conn

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.pool.release(self.conn)


@enum.unique
class _State(enum.IntEnum):
    not_connected = enum.auto()
    connected = enum.auto()
    error = enum.auto()
