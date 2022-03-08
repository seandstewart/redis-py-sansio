from __future__ import annotations

from typing import Dict, Iterable, List, Tuple, TypedDict, Union

from redis.sansio.callbacks.generic import pairs_to_dict
from redis.sansio.types import EncodedT


def parse_stream_list(response: RawStreamResponseT | None) -> ParsedStreamResponseT:
    if response is None:
        return None
    return [
        (r[0], pairs_to_dict(r[1])) if r is not None else (None, None) for r in response
    ]


def parse_xclaim(
    response: RawStreamResponseT | None, *, parse_justid: bool = False, **_
) -> ParsedStreamResponseT:
    if parse_justid:
        return response
    return parse_stream_list(response)


def parse_xinfo_stream(
    response: Iterable[EncodedT],
) -> dict[str, EncodedT | tuple[EncodedT, dict[EncodedT, EncodedT]] | None]:
    data = pairs_to_dict(response, decode_keys=True)
    first = data["first-entry"]
    if first is not None:
        data["first-entry"] = (first[0], pairs_to_dict(first[1]))
    last = data["last-entry"]
    if last is not None:
        data["last-entry"] = (last[0], pairs_to_dict(last[1]))
    return data


def parse_xread(
    response: list[tuple[EncodedT, RawStreamResponseT]] | None
) -> list[tuple[EncodedT, ParsedStreamResponseT]]:
    if response is None:
        return []
    return [(name, parse_stream_list(entries)) for name, entries in response]


def parse_xpending(
    response: Iterable[EncodedT] | _RawXPendingDetailT,
    *,
    parse_detail: bool = False,
    **_
) -> XPendingDetail | XPendingRangeEntry:
    if parse_detail:
        return parse_xpending_range(response)
    consumers = [{"name": n, "pending": int(p)} for n, p in response[3] or []]
    return {
        "pending": response[0],
        "min": response[1],
        "max": response[2],
        "consumers": consumers,
    }


_RawXPendingDetailT = Tuple[EncodedT, EncodedT, EncodedT, Iterable[Tuple[EncodedT]]]


class XPendingDetail(TypedDict):
    pending: int
    min: str
    max: str
    consumers: list[XPendingDetailConsumer]


class XPendingDetailConsumer(TypedDict):
    name: str | bytes
    pending: int


def parse_xpending_range(response) -> list[XPendingRangeEntry]:
    return [dict(zip(_XPENDING_RANGE_KEYS, r)) for r in response]


_XPENDING_RANGE_KEYS = (
    "message_id",
    "consumer",
    "time_since_delivered",
    "times_delivered",
)


class XPendingRangeEntry(TypedDict):
    message_id: str | bytes
    consumer: str | bytes
    time_since_deliveree: str | bytes
    times_delivered: str | bytes


RawStreamResponseT = Iterable[Tuple[EncodedT, Iterable[EncodedT]]]
ParsedStreamResponseT = List[
    Union[Tuple[None, None], Tuple[EncodedT, Dict[EncodedT, EncodedT]]]
]
