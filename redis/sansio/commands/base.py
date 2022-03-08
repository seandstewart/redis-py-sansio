from typing import Callable, Protocol

from redis.sansio.types import EncodableT, EncodedT


class CommandsProtocol(Protocol):
    def execute_command(self, *args, **kwargs):
        ...

    def get_encoder(self) -> Callable[[EncodableT], EncodedT]:
        ...
