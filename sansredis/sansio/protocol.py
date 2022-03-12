from __future__ import annotations

import socket
import ssl
from typing import Any, Mapping, NamedTuple, NoReturn, Tuple, Union

import attr

from sansredis.sansio import constants, events, exceptions, operator, types
from sansredis.sansio.callbacks import generic


class SansIORedisProtocol:
    """The Sans-IO Redis Protocol manages IO-agnostic logic for Redis Client<->Server communication.

    The main use-cases for the protocol are:

        1. Housing configuration objects for communication between client and server.
        2. Compiling commands into bytes which the Server will recognize.
        3. Reading responses from the Server and executing attached response callbacks.
        4. Defining logic for initializing and maintaining connections on the Server.
    """

    __slots__ = (
        "address_info",
        "client_info",
        "socket_info",
        "pool_info",
        "ssl_info",
        "operator",
    )

    def __init__(
        self,
        *,
        address_info: AddressInfo = None,
        client_info: ClientInfo = None,
        socket_info: SocketInfo = None,
        ssl_info: SSLInfo = None,
        use_ssl: bool = False,
        pool_info: PoolInfo = None,
    ):
        self.address_info = address_info or AddressInfo()
        self.client_info = client_info or ClientInfo()
        if self.client_info.decode_responses and self.client_info.encoding is None:
            self.client_info.encoding = "utf-8"
        if isinstance(self.client_info.server_version, str):
            self.client_info.server_version = version(self.client_info.server_version)
        self.socket_info = socket_info or SocketInfo()
        self.pool_info = pool_info or PoolInfo()
        self.ssl_info = ssl_info or SSLInfo() if use_ssl else None

        operator_cls = (
            operator.RESP2RedisOperator
            if self.client_info.resp_version == "2"
            else operator.RedisOperator
        )
        self.operator: operator.RedisOperator = operator_cls(
            notEnoughData=self.client_info.sentinel_value,
            encoding=self.client_info.encoding,
            errors=self.client_info.encoding_errors,
        )

    @staticmethod
    def make_command(
        command: str,
        *args,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ) -> events.Command:
        return events.Command(
            command=command,
            modifiers=[*args],
            callback=callback,
            callback_kwargs=callback_kwargs,
        )

    @staticmethod
    def make_pipeline(
        commands: list[events.Command] | None = None,
        transaction: bool = False,
        raise_on_error: bool = False,
    ):
        return events.PipelinedCommands(
            commands=commands,
            transaction=transaction,
            raise_on_error=raise_on_error,
        )

    def extend_pipeline(
        self,
        command: str,
        *args,
        pipeline: events.PipelinedCommands,
        callback: types.ResponseHandlerT = None,
        **callback_kwargs,
    ):
        event = self.make_command(
            command=command,
            *args,
            callback=callback,
            **callback_kwargs,
        )
        pipeline.commands.extend(event)

    def pack_command(
        self,
        event: events.Command | events.PipelinedCommands,
    ) -> events.PackedCommand:
        return self.operator.pack_command(event=event)

    def connection_error(self, exception: BaseException):
        # args for socket.error can either be (errno, "message")
        # or just "message"
        if len(exception.args) == 1:
            message = (
                f"{exception.__class__.__name__} while connecting to "
                f"{self.address_info.host}:{self.address_info.port}. "
                f"{exception.args[0]}."
            )
        else:
            message = (
                f"Error {exception.args[0]} connecting to "
                f"{self.address_info.host}:{self.address_info.port}. "
                f"{exception.args[1]}."
            )

        return exceptions.RedisConnectionError(message)

    def get_on_connect_routine(self) -> OnConnectRoutineT:
        # if username and/or password are set, authenticate
        command_stack = []
        address = self.address_info
        username, password = address.username, address.password
        cname = self.client_info.name
        server_version = self.client_info.server_version or ServerVersion()
        # If we're on 6.0, we can use HELLO to set the version, auth, and set the name.
        #   See: https://redis.io/commands/hello
        init = None
        if server_version >= self._SUPPORTS_HELLO:
            if self.client_info.resp_version is None:
                self.client_info.resp_version = "3"

            init = events.Command(
                command="HELLO",
                modifiers=[self.client_info.resp_version],
                callback=None,
                callback_kwargs={},
            )
            if username or password:
                init.modifiers.extend(("AUTH", username or "default", password or ""))
            if cname:
                init.modifiers.extend(("SETNAME", cname))

        # Otherwise, do it the old-fashioned way.
        else:
            # Force RESP2 if we're under 6.0.0
            self.client_info.resp_version = "2"
            self.operator = operator.RESP2RedisOperator(
                notEnoughData=self.client_info.sentinel_value,
                encoding=self.client_info.encoding,
                errors=self.client_info.encoding_errors,
            )
            if password:
                init = events.Command(
                    command="AUTH",
                    modifiers=[password],
                    callback=None,
                    callback_kwargs={},
                )

            if cname:
                setname = events.Command(command="CLIENT SETNAME", modifiers=[cname])
                command_stack.append(setname)
        # Set the database if we've provided one.
        db = address.db
        if db is not None:
            select = events.Command(
                command="SELECT", modifiers=[db], callback=None, callback_kwargs={}
            )
            command_stack.append(select)

        packed_init = self.pack_command(init) if init else init
        packed_stack = None
        if command_stack:
            if len(command_stack) > 1:
                packed_stack = self.pack_command(
                    self.make_pipeline(command_stack, raise_on_error=True)
                )
            else:
                packed_stack = self.pack_command(command_stack.pop())
        return packed_init, packed_stack

    _SUPPORTS_HELLO = (6, 0, 0)

    def should_check_health(self, curtime: int) -> bool:
        cinfo = self.client_info
        if cinfo.health_check_interval and curtime < cinfo.next_health_check:
            return True

    def get_health_check(self) -> events.Command:
        return self.make_command(
            b"PING", callback=lambda val: generic.str_if_bytes(val.lower()) == "pong"
        )

    def check_health_response(self, response: events.Response):
        if not response.reply:
            raise exceptions.RedisConnectionError(
                "Bad response from PING health check."
            )
        return True

    def set_next_health_check(self, curtime: int) -> NoReturn:
        self.client_info.next_health_check = curtime

    def configure_socket(self, sock: socket.socket, *, settimeout: bool = True):
        if settimeout:
            sock.settimeout(self.socket_info.timeout)
        if self.socket_info.is_unix_socket:
            return

        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        # TCP_KEEPALIVE
        if self.socket_info.keepalive:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
                for k, v in self.socket_info.keepalive_options.items():
                    sock.setsockopt(socket.SOL_TCP, k, v)
            except (OSError, TypeError):
                # `socket_keepalive_options` might contain invalid options
                # causing an error. Do not leave the operator open.
                sock.close()
                raise


def version(vstr: str) -> ServerVersion:
    return ServerVersion(*(int(v) for v in vstr.split(".")))


@attr.s(kw_only=True, slots=True, auto_attribs=True)
class AddressInfo:
    host: str = "localhost"
    port: int = 6379
    db: int | None = None
    password: str | None = None
    username: str | None = None


@attr.s(kw_only=True, slots=True, auto_attribs=True)
class ClientInfo:
    name: str | None = None
    encoding: str | None = None
    encoding_errors: str | None = None
    decode_responses: bool = False
    health_check_interval: float = 0
    next_health_check: float = float("inf")
    resp_version: types.RESPVersionT | None = None
    server_version: ServerVersion | None = None
    sentinel_value: Any = constants.SENTINEL


@attr.s(kw_only=True, slots=True, auto_attribs=True)
class SocketInfo:
    timeout: float | None = attr.field(factory=socket.getdefaulttimeout)
    connect_timeout: float | None = attr.field(factory=socket.getdefaulttimeout)
    retry_on_timeout: bool = False
    keepalive: bool = False
    keepalive_options: Mapping[int, int | bytes] | None = None
    type: int = 0
    read_size: int = 4096
    is_unix_socket: bool = False


@attr.s(kw_only=True, slots=True, auto_attribs=True)
class PoolInfo:
    min_connections: int = 0
    max_connections: int = 64
    pre_fill: bool = True
    block: bool = True


class ServerVersion(NamedTuple):
    major: int = 0
    minor: int = 0
    patch: int = 0


def _check_cert_reqs(value: str | int) -> int:
    if value is None:
        return
    if value in CERT_REQS:
        return value
    if value not in CERT_REQS_MAP:
        raise exceptions.RedisError(
            f"Invalid SSL Certificate Requirements Flag: {value!r}. "
            f"Expected one of {tuple(CERT_REQS_MAP)} or {tuple(CERT_REQS)}."
        )
    return value


@attr.s(kw_only=True, slots=True, auto_attribs=True)
class SSLInfo:
    keyfile: str | None = None
    certfile: str | None = None
    ca_certs: str | None = None
    check_hostname: bool = False
    cert_reqs: int | None = attr.field(default=None, converter=_check_cert_reqs)
    context: ssl.SSLContext | None = attr.field(init=False, default=None)

    def get_context(self) -> ssl.SSLContext:
        if not self.context:
            context = ssl.create_default_context()
            context.check_hostname = self.check_hostname
            context.verify_mode = self.cert_reqs
            if self.certfile and self.keyfile:
                context.load_cert_chain(certfile=self.certfile, keyfile=self.keyfile)
            if self.ca_certs:
                context.load_verify_locations(self.ca_certs)
            self.context = context
        return self.context


CERT_REQS_MAP = {
    "none": ssl.CERT_NONE,
    "optional": ssl.CERT_OPTIONAL,
    "required": ssl.CERT_REQUIRED,
}
CERT_REQS = frozenset(CERT_REQS_MAP.values())


OnConnectRoutineT = Tuple[
    Union[events.PackedCommand, None], Union[events.PackedCommand, None]
]
