from __future__ import annotations

from typing import Iterable

from redis.sansio.callbacks.generic import pairs_to_dict
from redis.sansio.types import EncodedT

from .zset import ScoreCastFuncT, ScorePairT


def parse_scan(response: tuple[EncodedT, EncodedT], **_) -> tuple[int, EncodedT]:
    cursor, r = response
    return int(cursor), r


def parse_hscan(
    response: tuple[EncodedT, Iterable[EncodedT] | None], **_
) -> tuple[int, dict[EncodedT, EncodedT]]:
    cursor, r = response
    return int(cursor), r and pairs_to_dict(r) or {}


def parse_zscan(
    response: tuple[EncodedT, Iterable[EncodedT]],
    *,
    score_cast_func: ScoreCastFuncT = float,
    **_
) -> tuple[int, list[ScorePairT]]:
    cursor, r = response
    it = iter(r)
    return int(cursor), [*zip(it, (score_cast_func(i) for i in it))]
