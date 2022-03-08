from __future__ import annotations

from typing import Callable

from redis.sansio.types import EncodedT


def parse_georadius_generic(
    response: EncodedT | list[EncodedT],
    *,
    store: bool = False,
    store_dist: bool = False,
    withdist: bool = False,
    withcoord: bool = False,
    withhash: bool = False,
    **_
):
    if store or store_dist:
        # `store` and `store_diff` cant be combined
        # with other command arguments.
        return response

    if type(response) != list:
        response_list = [response]
    else:
        response_list = response

    if (withdist, withcoord, withhash) == (False, False, False):
        # just a bunch of places
        return response_list

    casts: dict[str, Callable] = {
        "withdist": float,
        "withcoord": lambda ll: (float(ll[0]), float(ll[1])),
        "withhash": int,
    }
    if not withdist:
        casts.pop("withdist")
    if not withcoord:
        casts.pop("withcoord")
    if not withhash:
        casts.pop("withhash")

    # zip all output results with each casting function to get
    # the properly native Python value.
    f = [*casts.values()]
    castgen = (zip(f, r) for r in response_list)
    return [f(r) for f, r in castgen]
