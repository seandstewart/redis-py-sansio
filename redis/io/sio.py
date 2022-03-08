from __future__ import annotations

import collections
import enum
import socket
import threading
import time
from concurrent import futures
from typing import NoReturn

from redis.io import base
from redis.io.base import _RT
from redis.sansio import constants, events, exceptions, protocol, types
from redis.sansio.callbacks.resp2 import meta


class SyncIORedisConnectionPool(
    base.BaseIORedisConnectionPool["SyncIORedisConnection", types.ResponseBodyT]
):
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

    def _get_waiter(self):
        return threading.Condition()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect(inuse=True)

    def _wait_execute_command(
        self,
        command: str,
        *args,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ):
        with self.connection() as conn:
            return conn.execute_command(
                command,
                *args,
                callback=callback,
                **callback_kwargs,
            )

    def _wait_execute_pipeline(self, event: events.PipelinedCommands):
        with self.connection() as conn:
            return conn.execute_pipeline(event=event)

    def connection(self) -> _SyncIOPoolConnectionContext:
        """Check out a new connection from the pool.

        May be used as an context-manager or directly `await`-ed.
        """
        return _SyncIOPoolConnectionContext(pool=self)

    def acquire(self) -> SyncIORedisConnection:
        """Manually acquire a connection from the pool.

        If `max_connections` has been reached, this method will block until a
        connection is released back to the pool.
        """
        with self._connection_waiter:
            while True:
                # Add at least one connection to the pool, if possible.
                self.fill(override_min=True)
                # If we have available connection(s), grab one.
                if self.available():
                    conn = self.free.popleft()
                    self.inuse.add(conn)
                    return conn
                # Otherwise, wait until a connection is released.
                else:
                    self._connection_waiter.wait()

    def _wakeup(self):
        # Notify any connection waiters that they can check for a connection.
        with self._connection_waiter:
            self._connection_waiter.notify()

    def release(self, connection: SyncIORedisConnection):
        """Release a connection back into the pool.

        If we don't own this connection, it will be disconnected and discarded.

        Args:
            connection: The checked-out connection.
        """
        if not connection:
            return
        if connection not in self.inuse:
            connection.disconnect()
            return
        self.inuse.remove(connection)
        if connection.is_connected:
            self.free.append(connection)
        self._wakeup()

    def fill(self, *, override_min: bool = False):
        """Fill the pool to at least the min connection count.

        If `override_min` is `True`, keep adding active connections until there
        is a free one in the pool, until we hit the `max_connections` value.

        Args:
            override_min: Whether to keep adding connections after we've hit the minimum.
        """
        for conn in self.iterconn(override_min):
            conn.connect()
            self.free.append(conn)
        self._wakeup()

    def disconnect(self, *, inuse: bool = False):
        """Disconnect all free connections in the pool.

        If `inuse` is `True`, then also shutdown all checked out connections.

        Args:
            inuse: Whether we should close all checked out connections as well.

        Returns:

        """
        with self._connection_waiter:
            exc = []
            while self.free:
                conn: SyncIORedisConnection = self.free.pop()
                try:
                    conn.disconnect()
                except Exception as e:
                    exc.append(e)
            while inuse and self.inuse:
                conn: SyncIORedisConnection = self.inuse.pop()
                try:
                    conn.disconnect()
                except Exception as e:
                    exc.append(e)
            exc = next(iter(exc), None)
            if exc:
                raise exc

    def reset(self, *, inuse: bool = False):
        """Discard all currnt connections and fill the pool with new ones.

        Args:
            inuse: Whether we should close all checked out connections as well.
        """
        self.disconnect(inuse=inuse)
        self.fill()

    def make_connection(self) -> SyncIORedisConnection:
        """A connection factory."""
        return SyncIORedisConnection(protocol=self.protocol)


class SyncIORedisConnection(base.BaseIORedisConnection[types.ResponseBodyT]):
    """A Redis-compliant connection object.

    The `SyncIORedisConnection` manages an open `asyncio.Transport` and an
    `asyncio.Protocol` subclass which uses the `RedisOperator` class to manage the
    request<->response lifecycle for a Redis Client and Server.
    """

    def __init__(self, *, protocol: protocol.SansIORedisProtocol = None):
        super().__init__(
            protocol=protocol,
        )
        self._connectlock = threading.Lock()
        self._ioprotocol = RedisSyncIOProtocol(
            self.protocol,
        )
        self.connection: socket.socket | None = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def read_response(
        self, future: futures.Future
    ) -> events.Response | events.PipelinedResponses:
        try:
            response = future.result(timeout=self.protocol.socket_info.timeout)
        except futures.TimeoutError:
            raise exceptions.RedisTimeoutError("Timed out waiting for response.")
        if isinstance(response, Exception):
            raise response from None
        return response

    def connect(self):
        """Connects to the Redis server if not already connected"""
        if self.is_connected:
            return

        with self._connectlock:
            if self.is_connected:
                return
            try:
                self._connect()
            except socket.timeout:
                raise exceptions.RedisTimeoutError("Timeout connecting to server")
            except OSError as e:
                raise self.protocol.connection_error(e) from None
            except Exception as exc:
                raise self.protocol.connection_error(exc) from exc

            try:
                self.on_connect()
            except exceptions.RedisError:
                # clean up after any error in on_connect
                self.disconnect()
                raise

    def _connect(self):
        timeout = self.protocol.socket_info.connect_timeout
        address = self.protocol.address_info
        if self.protocol.socket_info.is_unix_socket:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(timeout)
            sock.connect(address.host)
        else:
            self.connection = socket.create_connection(
                address=(address.host, address.port),
                timeout=timeout,
            )
        self._ioprotocol.connection_made(self.connection)

    def on_connect(self):
        """Initialize the operator, authenticate and select a database"""
        version = self.protocol.client_info.server_version
        routine = self.connect_routine
        if version is None:
            info = self.execute_command("INFO", "server", callback=meta.parse_info)
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
                iresponse = self.send_command(event=init)
                return iresponse
            except exceptions.AuthenticationError as e:
                raise e from None
            except exceptions.ResponseError as e:
                raise self.protocol.connection_error(e) from e
        if stack:
            try:
                self.send_command(event=stack)
            except exceptions.ResponseError as e:
                raise self.protocol.connection_error(e) from e

    def disconnect(self):
        """Disconnects from the Redis server"""
        if not self.is_connected:
            return
        try:
            self.connection.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        try:
            self.connection.close()
        except OSError:
            pass
        self._ioprotocol.connection_lost(None)
        self.connection = None

    def check_health(self):
        """Check the health of the operator with a PING/PONG"""
        if self.protocol.should_check_health(time.monotonic()):
            command = self.protocol.get_health_check()
            try:
                response = self.read_response(self.send_command(command))
                self.protocol.check_health_response(response)
            except (ConnectionError, TimeoutError) as err:
                self.disconnect()
                try:
                    response = self.read_response(self.send_command(command))
                    self.protocol.check_health_response(response)
                except BaseException as err2:
                    raise err2 from err

            self.protocol.set_next_health_check(time.monotonic())

    def _do_send_and_read_pipeline(self, event: events.PackedCommand) -> _RT:
        fut = self.send_command(event)
        response = self.read_response(fut)
        return response.replies

    def _do_send_and_read_command(self, event: events.PackedCommand) -> _RT:
        fut = self.send_command(event)
        response = self.read_response(fut)
        return response.reply


class RedisSyncIOProtocol:
    __slots__ = (
        "operator",
        "proto",
        "_state",
        "_waiters",
        "_transport",
        "_exc",
        "_conn_waiter",
        "_data_waiter",
        "_disconnect_waiter",
        "_reader",
    )

    def __init__(
        self,
        proto: protocol.SansIORedisProtocol,
    ):
        self.operator = proto.operator
        self.proto = proto
        self._state = _State.not_connected
        self._waiters = collections.deque()
        self._transport: socket.socket | None = None
        self._exc: BaseException | None = None
        self._conn_waiter: threading.Event = threading.Event()
        self._data_waiter: threading.Condition = threading.Condition()
        self._disconnect_waiter: threading.Event = threading.Event()
        self._reader: futures.ThreadPoolExecutor | None = None

    @property
    def is_connected(self):
        return self._state == _State.connected and self._transport

    def connection_made(self, transport: socket.socket) -> None:
        self.proto.configure_socket(transport)
        self._transport = transport
        self._state = _State.connected
        self._reader = futures.ThreadPoolExecutor(max_workers=1)
        self._reader.submit(self.read_forever)
        self._conn_waiter.set()

    def wait_connected(self) -> None:
        """Wait to access the operator until `connection_made` is complete."""
        self._conn_waiter.wait()

    def wait_disconnected(self):
        self._disconnect_waiter.wait()

    def send_command(self, event: events.PackedCommand) -> futures.Future:
        if self._state == _State.connected:
            future = futures.Future()
            self._transport.sendall(event.payload)
            self._waiters.append((event.command, future))
            return future

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

    def read_forever(self):
        """Read from the socket in a loop.

        This is intended to be scheduled in a separate thread from the main client.
        """
        # Make sure the connection is set up.
        self.wait_connected()
        while True:
            # See if there's any data on the socket and pass to the parser.
            has_data = self._read_from_socket(raise_on_timeout=False)
            if has_data:
                # If we've passed to the parser,
                #   read the responses and pipe them into the waiting Futures.
                self._read_response()
            # Sleep this thread so we can context-switch.
            time.sleep(0)

    def _read_from_socket(
        self, timeout: float = ..., raise_on_timeout: bool = True
    ) -> bool:
        try:
            if timeout is not ...:
                self._transport.settimeout(timeout)
            data = self._transport.recv(self.proto.socket_info.read_size)
            if not isinstance(data, bytes) or len(data) == 0:
                exc = exceptions.RedisConnectionError(
                    constants.SERVER_CLOSED_CONNECTION_ERROR
                )
                self._set_exception(exc)
                raise exc
            self.operator.receive_data(data)
            return True
        except socket.timeout:
            if raise_on_timeout:
                exc = exceptions.RedisTimeoutError("Timeout reading from socket")
                self._set_exception(exc)
                raise exc
            return False
        except constants.NONBLOCKING_EXCEPTIONS as ex:
            # if we're in nonblocking mode and the recv raises a
            # blocking error, simply return False indicating that
            # there's no data to be read. otherwise raise the
            # original exception.
            allowed = constants.NONBLOCKING_EXCEPTION_ERROR_NUMBERS.get(
                ex.__class__, -1
            )
            if not raise_on_timeout and ex.errno == allowed:
                return False
            exc = exceptions.RedisConnectionError(
                f"Error while reading from socket: {ex.args}"
            )
            self._set_exception(exc)
            raise exc
        finally:
            if timeout is ... and self._state == _State.connected:
                self._transport.settimeout(self.proto.socket_info.timeout)

    def _read_response(self) -> NoReturn:
        if self._state != _State.connected or not self._waiters:
            return
        _get_waiter = self._get_waiter
        read = self.operator.read_response
        for parsed in self.operator.iterparse():
            item = _get_waiter()
            if item is None:
                continue
            cmd, fut = item
            response = read(cmd, parsed)
            fut.set_result(response)

    def _get_waiter(self) -> tuple[events.Command, futures.Future] | None:
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
        if exc is not None:
            self._set_exception(exc)

        elif self._state == _State.connected:
            exc = ConnectionError("Lost connection to server.")
            self._state = _State.not_connected
            self._set_exception(exc)

        self._reader.shutdown(wait=True)
        self._disconnect_waiter.set()

    def _set_exception(self, exc):
        self._exc = exc
        self._state = _State.error


class _SyncIOPoolConnectionContext:
    def __init__(self, pool: SyncIORedisConnectionPool):
        self.pool = pool
        self.conn = None

    def __enter__(self):
        self.conn = self.pool.acquire()
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.release(self.conn)


@enum.unique
class _State(enum.IntEnum):
    not_connected = enum.auto()
    connected = enum.auto()
    error = enum.auto()
