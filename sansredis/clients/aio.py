from __future__ import annotations

import contextlib
import itertools
from typing import Any, AsyncIterator, Iterable

from sansredis.clients.base import BaseRedis, PipelineMixin
from sansredis.io import aio


class AsyncIORedis(BaseRedis[aio.AsyncIORedisConnectionPool]):

    def make_pool(self) -> aio.AsyncIORedisConnectionPool:
        return aio.AsyncIORedisConnectionPool(protocol=self.protocol)

    async def connect(self):
        if self.single_connection_client:
            await self.connection.connect()
        elif self.protocol.pool_info.pre_fill:
            await self.connection_pool.fill()

    async def disconnect(self, *, inuse: bool = True):
        if self.connection:
            await self.connection.disconnect()
        if self.auto_close_connection_pool:
            await self.connection_pool.disconnect(inuse=inuse)

    def __await__(self):
        return self.connect().__await__()

    async def __aenter__(self):
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.disconnect(inuse=True)


class AsyncIOPipeline(AsyncIORedis, PipelineMixin):

    async def _do_watch(self, *names: str | bytes):
        if not self.connection:
            self.connection = await self.connection_pool.acquire()
        return await self.connection.execute_command("WATCH", *names)

    async def _do_release_connection(self, conn: aio.AsyncIORedisConnectionPool):
        try:
            await conn.execute_command("UNWATCH")
        except ConnectionError:
            await conn.disconnect()
        await self.connection_pool.release(conn)
