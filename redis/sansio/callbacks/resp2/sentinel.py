from __future__ import annotations

from typing import Iterable, Sequence

from redis.sansio.callbacks.generic import pairs_to_dict_typed, str_if_bytes
from redis.sansio.types import EncodedT


def parse_sentinel_state(item: Iterable[str]):
    result = pairs_to_dict_typed(item, SENTINEL_STATE_TYPES)
    flags = {*result["flags"].split(",")}
    for name, flag in (
        ("is_master", "master"),
        ("is_slave", "slave"),
        ("is_sdown", "s_down"),
        ("is_odown", "o_down"),
        ("is_sentinel", "sentinel"),
        ("is_disconnected", "disconnected"),
        ("is_master_down", "master_down"),
    ):
        result[name] = flag in flags
    return result


def parse_sentinel_master(response: Iterable[EncodedT]) -> dict[str, str | int | bool]:
    return parse_sentinel_state((str_if_bytes(r) for r in response))


def parse_sentinel_masters(
    response: Iterable[EncodedT],
) -> dict[str, dict[str, str | int | bool]]:
    strgen = ((str_if_bytes(it) for it in r) for r in response)
    dictgen = (parse_sentinel_state(r) for r in strgen)
    return {state["name"]: state for state in dictgen}


def parse_sentinel_slaves_and_sentinels(
    response: Iterable[EncodedT],
) -> list[dict[str, str | int | bool]]:
    return [parse_sentinel_state(str_if_bytes(it) for it in item) for item in response]


def parse_sentinel_get_master(
    response: Sequence[EncodedT],
) -> tuple[EncodedT, int] | None:
    return response and (response[0], int(response[1])) or None


SENTINEL_STATE_TYPES = {
    "can-failover-its-master": int,
    "config-epoch": int,
    "down-after-milliseconds": int,
    "failover-timeout": int,
    "info-refresh": int,
    "last-hello-message": int,
    "last-ok-ping-reply": int,
    "last-ping-reply": int,
    "last-ping-sent": int,
    "master-link-down-time": int,
    "master-port": int,
    "num-other-sentinels": int,
    "num-slaves": int,
    "o-down-time": int,
    "pending-commands": int,
    "parallel-syncs": int,
    "port": int,
    "quorum": int,
    "role-reported-time": int,
    "s-down-time": int,
    "slave-priority": int,
    "slave-repl-offset": int,
    "voted-leader-epoch": int,
}
