from __future__ import annotations

from typing import Iterable

from sansredis.sansio.types import EncodedT


def parse_pubsub_numsub(
    response: Iterable[EncodedT], **_
) -> list[tuple[EncodedT, EncodedT]]:
    it = iter(response)
    return [*zip(it, it)]
