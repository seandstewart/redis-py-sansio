from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, Iterable, Mapping, Type, TypeVar, Union

from sansredis.sansio.types import ArgT, DecodedT, EncodedT, KeyT


def list_or_args(
    keys: KeyT | Iterable[KeyT], args: Iterable[ArgT] | None
) -> list[KeyT | ArgT]:
    """Combine a key or collection of keys and any args into a single list."""
    key_list: list[KeyT | ArgT] = (
        [keys, *(args or [])]
        if isinstance(keys, (bytes, str, memoryview))
        else [*keys, *(args or [])]
    )
    return key_list


def str_if_bytes(val: str | bytes | bytearray | memoryview) -> str:
    """If a value is bytes, decode to string."""
    if isinstance(val, memoryview):
        return val.tobytes().decode(errors="replace")
    return val.decode(errors="replace") if isinstance(val, bytes) else val


def timestamp_to_datetime(response: EncodedT) -> datetime.datetime | None:
    """Converts a unix timestamp to a Python datetime object."""
    isnumber, isstring = isinstance(response, _NUMBERS), isinstance(
        response, _STRINGLIKE
    )
    if not response or False in (isnumber, isstring):
        return None
    if isstring and not response.isdigit():
        return None

    return datetime.datetime.fromtimestamp(int(response))


def int_or_none(response: EncodedT | None) -> int | None:
    return response and int(response)


def float_or_none(response: EncodedT | None) -> float | None:
    return response and int(response)


def bool_ok(response: EncodedT) -> bool:
    return str_if_bytes(response).lower() == "ok"


_NUMBERS = (int, float)
_BYTESLIKE = (bytes, bytearray)
_STRINGLIKE = (str, memoryview, *_BYTESLIKE)


def pairs_to_dict(
    response: Iterable[EncodedT] | None,
    decode_keys: bool = False,
    decode_string_values: bool = False,
) -> _DictFromPairsT:
    """Take an iterable of even-numbered length and dump to a dictionary."""
    if response is None:
        return {}

    it = iter(response)
    truth = (decode_keys, decode_string_values)
    gen = zip(it, it)
    if truth == (True, True):
        gen = ((str_if_bytes(k), str_if_bytes(v)) for k, v in zip(it, it))
    elif truth == (True, False):
        gen = ((str_if_bytes(k), v) for k, v in zip(it, it))
    elif truth == (False, True):
        gen = ((k, str_if_bytes(v)) for k, v in zip(it, it))

    d = {k: v for k, v in gen}
    return d


_DictFromPairsT = Union[
    Dict[DecodedT, DecodedT],
    Dict[EncodedT, DecodedT],
    Dict[DecodedT, EncodedT],
    Dict[EncodedT, EncodedT],
]


def pairs_to_dict_with_str_keys(
    response: Iterable[EncodedT],
) -> dict[DecodedT, EncodedT]:
    return pairs_to_dict(response, decode_keys=True)


def parse_list_of_dicts(response: Iterable[EncodedT]) -> list[dict[DecodedT, EncodedT]]:
    return [pairs_to_dict_with_str_keys(e) for e in response]


def list_of_dicts(response: Iterable[EncodedT]) -> list[_DictFromPairsT]:
    return [pairs_to_dict_with_str_keys(r) for r in response]


def pairs_to_dict_typed(
    response: Iterable[EncodedT], type_info: Mapping[KeyT, CoercerT]
):
    """Take an iterable of even-numbered length and dump to a dictionary."""
    it = iter(response)
    return {
        key: (maybe_coerce(type_info[key](value)) if key in type_info else value)
        for key, value in zip(it, it)
    }


def maybe_coerce(val: EncodedT, target: CoercerT[_T]) -> _T:
    try:
        return target(val)
    # if for some reason the value can't be coerced, just use
    # the string value
    except (ValueError, TypeError):
        return val


_T = TypeVar("_T")
CoercerT = Union[Callable[[Any], _T], Type[_T]]
