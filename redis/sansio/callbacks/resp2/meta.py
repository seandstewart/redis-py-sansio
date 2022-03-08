from __future__ import annotations

from typing import Any, Iterable, Iterator

from redis.sansio.callbacks.generic import int_or_none, pairs_to_dict, str_if_bytes
from redis.sansio.types import DecodedT, EncodedT


def parse_debug_object(response: EncodedT) -> dict[str, str | int]:
    """Parse the results of Redis's DEBUG OBJECT command into a Python dict"""
    # The 'type' of the object is the first item in the response, but isn't
    # prefixed with a name
    kvs = (kv.split(":") for kv in f"type:{str_if_bytes(response)}".split())
    return {k: int(v) if k in _INT_FIELDS else v for k, v in kvs}


_INT_FIELDS = frozenset(("refcount", "serializedlength", "lru", "lru_seconds_idle"))


def parse_object(
    response: EncodedT | None, infotype: EncodedT | None
) -> EncodedT | int | None:
    """Parse the results of an OBJECT command"""
    return int_or_none(response) if infotype in {"idletime", "refcount"} else response


def parse_info(response: EncodedT) -> dict[str, DecodedT | list[DecodedT]]:
    """Parse the result of Redis's INFO command into a Python dict"""
    info: dict[str, Any] = {}
    response = str_if_bytes(response)

    for key, value in _iter_info_kvs(response):
        if key == "__raw__":
            info.setdefault(key, []).append(value)
        if key == "module":
            info.setdefault("modules", []).append(value)
        info[key] = value

    return info


def _iter_info_kvs(response: str) -> Iterator[str, Any]:
    for line in response.splitlines():
        if line.startswith("#"):
            continue
        if ":" not in line:
            yield "__raw__", line
            continue
        key, value = line.split(":", maxsplit=1)
        if key == "cmdstat_host":
            key, value = line.rsplit(":", 1)
        value = _parse_info_value(value)
        yield key, value


def _parse_info_value(value: str):
    # If we don't have k,v pairs, parse as
    if "," not in value or "=" not in value:
        out = value
        try:
            out = float(value) if "." in value else int(value)
        except ValueError:
            return out
        else:
            return out

    gen = (item.split("=") for item in value.split(","))
    return {k: _parse_info_value(v) for k, v in gen}


def parse_memory_stats(response: EncodedT) -> dict[str, DecodedT]:
    stats = {
        k: pairs_to_dict(v, decode_keys=True, decode_string_values=True)
        if k.startswith("db.")
        else v
        for k, v in pairs_to_dict(
            response, decode_keys=True, decode_string_values=True
        ).items()
    }
    return stats


def parse_config_get(response: Iterable[EncodedT], **_) -> dict[str, EncodedT]:
    response = [str_if_bytes(i) if i is not None else None for i in response]
    return response and pairs_to_dict(response) or {}


def parse_slowlog_get(response, *, decode_responses: bool = False, **_):
    space: str | bytes = " " if decode_responses else b" "
    return [
        {
            "id": item[0],
            "start_time": int(item[1]),
            "duration": int(item[2]),
            # Redis Enterprise injects another entry at index [3], which has
            # the complexity info (i.e. the value N in case the command has
            # an O(N) complexity) instead of the command.
            "command": (
                space.join(item[3])
                if isinstance(item[3], list)
                else space.join(item[4])
            ),
        }
        for item in response
    ]
