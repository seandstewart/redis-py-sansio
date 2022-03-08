from __future__ import annotations

from typing import Any, Iterable

from redis.sansio.callbacks.generic import bool_ok, pairs_to_dict, str_if_bytes
from redis.sansio.types import EncodedT

__all__ = (
    "parse_client_list",
    "parse_client_kill",
    "parse_acl_getuser",
    "parse_acl_log",
    "parse_client_info",
)


def parse_client_list(response: EncodedT, **_) -> list[dict[str, str]]:
    lines = str_if_bytes(response).splitlines()
    groupgen = (c.split(" ") for c in lines)
    kvgen = (pair.split("=", maxsplit=1) for group in groupgen for pair in group)
    return [{k: v} for group in kvgen for k, v in group]


def parse_client_kill(response: int | EncodedT, **_) -> bool | int:
    if isinstance(response, int):
        return response
    return str_if_bytes(response) == "OK"


def parse_acl_getuser(response: Iterable[EncodedT], **_) -> dict[str, Any]:
    if response is None:
        return None
    data = pairs_to_dict(response, decode_keys=True)

    # convert everything but user-defined data in 'keys' to native strings
    data["flags"] = list(map(str_if_bytes, data["flags"]))
    data["passwords"] = list(map(str_if_bytes, data["passwords"]))
    data["commands"] = str_if_bytes(data["commands"])

    # split 'commands' into separate 'categories' and 'commands' lists
    commands, categories = [], []
    for command in data["commands"].split(" "):
        if "@" in command:
            categories.append(command)
        else:
            commands.append(command)

    data["commands"] = commands
    data["categories"] = categories
    data["enabled"] = "on" in data["flags"]
    return data


def parse_acl_log(response, **_):
    if response is None:
        return None
    if isinstance(response, list):
        data = []
        for log in response:
            log_data = pairs_to_dict(log, decode_keys=True, decode_string_values=True)
            client_info = log_data.get("client-info", "")
            log_data["client-info"] = parse_client_info(client_info)

            # float() is lossy comparing to the "double" in C
            log_data["age-seconds"] = float(log_data["age-seconds"])
            data.append(log_data)
    else:
        data = bool_ok(response)
    return data


def parse_client_info(value: str) -> dict[str, str | int]:
    """
    Parsing client-info in ACL Log in following format.
    "key1=value1 key2=value2 key3=value3"
    """
    client_info = {}
    infos = value.split(" ")
    for info in infos:
        key, value = info.split("=")
        client_info[key] = value

    # Those fields are definded as int in networking.c
    for int_key in {
        "id",
        "age",
        "idle",
        "db",
        "sub",
        "psub",
        "multi",
        "qbuf",
        "qbuf-free",
        "obl",
        "oll",
        "omem",
    }:
        client_info[int_key] = int(client_info[int_key])
    return client_info
