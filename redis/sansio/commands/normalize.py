from __future__ import annotations

from typing import Iterable

from redis.sansio.types import EncodableT, EncodedT


def iterkeysargs(
    keys: EncodedT | str | Iterable[EncodedT | str], args: Iterable[EncodableT]
) -> Iterable[EncodableT]:
    if isinstance(keys, (str, bytes, memoryview, bytearray)):
        yield keys
    else:
        yield from iter(keys)
    yield from iter(args)
