from __future__ import annotations

from redis.sansio.exceptions import ModuleError
from redis.sansio.types import EncodedT


def parse_module_result(response: EncodedT | ModuleError) -> bool:
    if isinstance(response, ModuleError):
        raise response
    return True
