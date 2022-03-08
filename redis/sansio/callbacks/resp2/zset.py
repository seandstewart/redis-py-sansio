from __future__ import annotations

import numbers
from typing import Callable, Iterable, Sequence, Tuple, TypeVar

from redis.sansio.types import EncodedT


def zset_score_pairs(
    response: Iterable[EncodedT] | None,
    *,
    withscores: bool = False,
    score_cast_func: Callable[[EncodedT], _NT] = float,
    **_
) -> list[tuple[EncodedT, _NT]]:
    """
    If ``withscores`` is specified in the options, return the response as
    a list of (value, score) pairs
    """
    if not response or not withscores:
        return response
    it = iter(response)
    return [*zip(it, (score_cast_func(i) for i in it))]


def sort_return_tuples(
    response: Sequence[EncodedT] | None, *, groups: int | None = None, **_
) -> list[tuple[EncodedT, ...]]:
    """
    If ``groups`` is specified, return the response as a list of
    n-element tuples with n being the value found in options['groups']
    """
    if not response or not groups:
        return response
    it = iter(response)
    groupit = [it] * groups
    return [*zip(groupit)]


def parse_zadd(
    response: EncodedT | None, *, as_score: bool = False, **_
) -> int | float | None:
    if response is None:
        return None
    return float(response) if as_score else int(response)


_NT = TypeVar("_NT", bound=numbers.Number)
ScoreCastFuncT = Callable[[EncodedT], _NT]
ScorePairT = Tuple[EncodedT, _NT]
