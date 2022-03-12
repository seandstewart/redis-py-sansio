from __future__ import annotations

from sansredis.sansio.exceptions import ModuleError
from sansredis.sansio.types import EncodedT


def parse_module_result(response: EncodedT | ModuleError) -> bool:
    if isinstance(response, ModuleError):
        raise response
    return True
