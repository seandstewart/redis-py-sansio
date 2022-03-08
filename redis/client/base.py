from __future__ import annotations

from typing import Any, Generic, Literal, Mapping, TypeVar

from redis.io import base
from redis.sansio import constants, exceptions
from redis.sansio import protocol as proto
from redis.sansio import types
from redis.sansio.commands import core

_RT = TypeVar("_RT")
_PT = TypeVar("_PT", bound=base.BaseIORedisConnectionPool)


class BaseRedis(core.CoreCommands, Generic[_PT]):
    connection_pool: _PT

    def __init__(
        self,
        *,
        connection_pool: base.BaseIORedisConnectionPool | None = None,
        protocol: proto.SansIORedisProtocol | None = None,
        host: str = "localhost",
        port: int = 6379,
        db: str | int = 0,
        username: str | None = None,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool | None = None,
        socket_keepalive_options: Mapping[int, int | bytes] = None,
        unix_socket_path: str | None = None,
        retry_on_timeout: bool = False,
        ssl: bool = False,
        ssl_keyfile: str | None = None,
        ssl_certfile: str | None = None,
        ssl_cert_reqs: str = "required",
        ssl_ca_certs: str | None = None,
        ssl_check_hostname: bool = False,
        min_connections: int = 10,
        max_connections: int = 64,
        health_check_interval: int = 0,
        client_name: str | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        resp_version: Literal["2", "3"] = "3",
        server_version: proto.ServerVersion | str | None = None,
        sentinel_value: Any = constants.SENTINEL,
        single_connection_client: bool = False,
        auto_close_connection_pool: bool = True,
    ):
        # auto_close_connection_pool only has an effect if connection_pool is
        # None. This is a similar feature to the missing __del__ to resolve #1103,
        # but it accounts for whether a user wants to manually close the connection
        # pool, as a similar feature to ConnectionPool's __del__.
        self.auto_close_connection_pool = (
            auto_close_connection_pool if connection_pool is None else False
        )
        if connection_pool:
            protocol = connection_pool.protocol
        if not protocol:
            protocol = proto.SansIORedisProtocol(
                address_info=proto.AddressInfo(
                    host=unix_socket_path or host,
                    port=port,
                    db=int(db),
                    password=password,
                    username=username,
                ),
                client_info=proto.ClientInfo(
                    name=client_name,
                    encoding=encoding,
                    encoding_errors=encoding_errors,
                    decode_responses=decode_responses,
                    health_check_interval=health_check_interval,
                    resp_version=resp_version,
                    server_version=server_version,
                    sentinel_value=sentinel_value,
                ),
                socket_info=proto.SocketInfo(
                    timeout=socket_timeout,
                    connect_timeout=socket_connect_timeout,
                    retry_on_timeout=retry_on_timeout,
                    keepalive=socket_keepalive,
                    keepalive_options=socket_keepalive_options,
                    is_unix_socket=bool(unix_socket_path),
                ),
                ssl_info=proto.SSLInfo(
                    keyfile=ssl_keyfile,
                    certfile=ssl_certfile,
                    ca_certs=ssl_ca_certs,
                    check_hostname=ssl_check_hostname,
                    cert_reqs=ssl_cert_reqs,
                ),
                use_ssl=ssl,
                pool_info=proto.PoolInfo(
                    min_connections=min_connections,
                    max_connections=max_connections,
                ),
            )
        self.protocol = protocol
        self.connection_pool = connection_pool or self.make_pool()
        self.single_connection_client = single_connection_client
        self.connection = None
        if self.single_connection_client:
            self.connection = self.connection_pool.make_connection()

    def get_encoder(self) -> types.EncoderT:
        return self.protocol.operator._writer.encode

    def make_pool(self) -> _PT:
        raise NotImplementedError()

    def connect(self):
        raise NotImplementedError()

    def disconnect(self, *, inuse: bool = True):
        raise NotImplementedError()

    def execute_command(
        self, command: str | bytes, *args, callback=None, **kwargs
    ) -> Any:
        if self.connection:
            return self.connection.execute_command(
                command, *args, callback=callback, **kwargs
            )
        return self.connection_pool.execute_command(
            command, *args, callback=callback, **kwargs
        )


_ClientT = TypeVar("_ClientT", bound=BaseRedis)


class PipelineMixin:

    def __init__(
        self: _ClientT,
        connection_pool: base.BaseIORedisConnectionPool,
        *,
        transaction: bool = False,
    ):
        super().__init__(connection_pool=connection_pool)
        self.watching = False
        self.explicit_transaction = transaction
        self.stack = self.protocol.make_pipeline(transaction=transaction)

    def execute_command(
        self: _ClientT, command: str | bytes, *args, callback=None, **kwargs
    ) -> _ClientT:
        if command == "WATCH":
            raise exceptions.RedisError("'WATCH' cannot be pipelined.")
        self.protocol.extend_pipeline(
            command, *args, pipeline=self.stack, callback=callback, **kwargs
        )
        return self

    def execute(self: _ClientT, *, raise_on_error: bool = True):
        # Reset the current stack.
        stack = self.stack
        stack.raise_on_error = raise_on_error
        self.stack = self.protocol.make_pipeline(
            transaction=stack.transaction
        )
        # We'll only bind to an explicit connection if we've called WATCH
        if self.connection:
            return self.connection.execute_pipeline(stack)
        return self.connection_pool.execute_pipeline(stack)

    def discard(self):
        return self.execute_command("DISCARD")

    def watch(self: _ClientT, *names: str | bytes):
        if self.explicit_transaction:
            raise exceptions.RedisError("Cannot issue a WATCH after a MULTI")
        return self._do_watch(*names)

    def multi(self):
        """
        Start a transactional block of the pipeline after WATCH commands
        are issued. End the transactional block with `execute`.
        """
        if self.explicit_transaction:
            return
        if self.stack.commands:
            raise exceptions.RedisError(
                "Commands without an initial WATCH have already " "been issued"
            )
        self.explicit_transaction = True

    def unwatch(self: _ClientT):
        return self.execute_command("UNWATCH")

    def reset(self: _ClientT):
        self.stack = self.protocol.make_pipeline(transaction=self.stack.transaction)
        self.watching = False
        conn = self.connection
        self.connection = None
        return self._do_release_connection(conn)

    def _do_release_connection(self, conn: base.BaseIORedisConnection):
        raise NotImplementedError()

    def _do_watch(self: _ClientT, *names: str | bytes):
        raise NotImplementedError()

