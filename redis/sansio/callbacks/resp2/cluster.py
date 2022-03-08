from __future__ import annotations

from typing import TypedDict

from redis.sansio.callbacks.generic import str_if_bytes
from redis.sansio.types import EncodedT


def parse_cluster_info(response: EncodedT, **_) -> dict[str, str]:
    return dict(line.split(":") for line in str_if_bytes(response).splitlines() if line)


def _parse_node_line(line: str) -> ClusterNodeInfo:
    line_items = line.split(" ")
    node_id, addr, flags, master_id, ping, pong, epoch, connected, *slots = line_items
    node_dict = {
        "node_id": node_id,
        "flags": flags,
        "master_id": master_id,
        "last_ping_sent": ping,
        "last_pong_rcvd": pong,
        "epoch": epoch,
        "slots": [sl.split("-") for sl in slots],
        "connected": True if connected == "connected" else False,
    }
    return addr, node_dict


def parse_cluster_nodes(response, **_) -> dict[str, ClusterNodeInfo]:
    raw_lines = str_if_bytes(response).splitlines()
    return dict(_parse_node_line(line) for line in raw_lines)


class ClusterNodeInfo(TypedDict):
    node_id: str
    flags: str
    master_id: str
    last_ping_sent: str
    last_pong_rcvd: str
    epoch: str
    slots: list[tuple[str, str]]
    connectec: bool
