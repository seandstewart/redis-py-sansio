from __future__ import annotations

import asyncio

import aioredis
import aredis
import pytest
import redis
import redis.asyncio.client

from sansredis.clients import aio, sio
from sansredis.sansio import protocol

proto = protocol.SansIORedisProtocol(
    pool_info=protocol.PoolInfo(
        min_connections=20, max_connections=100
    ),
    client_info=protocol.ClientInfo(
        resp_version="3",
        server_version="6.2",
        decode_responses=True,
    )
)


@pytest.fixture(scope="session")
def event_loop():
    return asyncio.new_event_loop()


@pytest.fixture(scope="session")
def redis_pool():
    r = redis.Redis(decode_responses=True)
    yield r
    r.connection_pool.disconnect()


@pytest.fixture(scope="session")
def redis_single(redis_pool):
    yield redis_pool.client()


@pytest.fixture(scope="session")
async def redis_aio_pool():
    r = redis.asyncio.client.Redis(decode_responses=True)
    yield r
    await r.connection_pool.disconnect()


@pytest.fixture(scope="session")
def redis_aio_single(redis_aio_pool):
    yield redis_aio_pool.client()


@pytest.fixture(scope="session")
async def aredis_pool():
    r = aredis.StrictRedis(decode_responses=True)
    yield r
    r.connection_pool.disconnect()


@pytest.fixture(scope="session")
async def sansredis_aio_pool():
    async with aio.AsyncIORedis(protocol=proto) as r:
        yield r


@pytest.fixture(scope="session")
def sansredis_sio_pool():
    with sio.SyncIORedis(protocol=proto) as r:
        yield r


@pytest.fixture(scope="session")
async def sansredis_aio_single():
    async with aio.AsyncIORedis(protocol=proto, single_connection_client=True) as r:
        yield r


@pytest.fixture(scope="session")
def sansredis_sio_single():
    with sio.SyncIORedis(protocol=proto, single_connection_client=True) as r:
        yield r


@pytest.fixture(scope="session")
async def aioredis_pool():
    r = await aioredis.create_redis_pool(
        "redis://localhost:6379", encoding='utf-8',
        minsize=proto.pool_info.min_connections, maxsize=proto.pool_info.max_connections
    )
    yield r
    r.close()
    await r.wait_closed()


@pytest.fixture(scope="session")
async def aioredis_single():
    r = await aioredis.create_redis("redis://localhost:6379", encoding='utf-8')
    yield r
    r.close()
    await r.wait_closed()


@pytest.fixture(scope="session")
def benches(
        aioredis_pool,
        aioredis_single,
        aredis_pool,
        redis_pool,
        redis_single,
        redis_aio_pool,
        redis_aio_single,
        sansredis_aio_pool,
        sansredis_aio_single,
        sansredis_sio_pool,
        sansredis_sio_single,
):
    return {
        "aioredis-pool": (aioredis_pool, bench_aioredis),
        "aioredis-single": (aioredis_single, bench_aioredis),
        "aredis-pool": (aredis_pool, bench_aredis),
        "redis-pool": (redis_pool, bench_redis_sio),
        "redis-single": (redis_single, bench_redis_sio),
        "redis-asyncio-pool": (redis_aio_pool, bench_redis_aio),
        "redis-asyncio-single": (redis_aio_single, bench_redis_aio),
        "sansredis-asyncio-pool": (sansredis_aio_pool, bench_sansio_aio),
        "sansredis-asyncio-single": (sansredis_aio_single, bench_sansio_aio),
        "sansredis-syncio-pool": (sansredis_sio_pool, bench_sansio_sio),
        "sansredis-syncio-single": (sansredis_sio_single, bench_sansio_sio),
    }


@pytest.fixture(
    params=[
        "aioredis-pool",
        "aioredis-single",
        "aredis-pool",
        "redis-pool",
        "redis-single",
        "redis-asyncio-pool",
        "redis-asyncio-single",
        "sansredis-asyncio-pool",
        "sansredis-asyncio-single",
        "sansredis-syncio-pool",
        "sansredis-syncio-single",
    ]
)
def bench_target(request, benches):
    return request.param, benches[request.param]


async def bench_sansio_aio(r: aio.AsyncIORedis, n: int = 1000):
    tasks = [
        asyncio.create_task(
            aio_task(i, r), name=f"resp3-{i}"
        ) for i in range(n)
    ]
    return await asyncio.gather(*tasks)


def bench_sansio_sio(r: sio.SyncIORedis, n: int = 1000):
    return [sio_task(i, r) for i in range(n)]


async def bench_redis_aio(r: redis.asyncio.client.Redis, n: int = 1000):
    tasks = [
        asyncio.create_task(
            aio_task(i, r), name=f"resp3-{i}"
        ) for i in range(n)
    ]
    return await asyncio.gather(*tasks)


def bench_redis_sio(r: redis.Redis, n: int = 1000):
    return [sio_task(i, r) for i in range(n)]


async def bench_aredis(r: aredis.StrictRedis, n: int = 1000):
    tasks = [
        asyncio.create_task(
            aio_task(i, r), name=f"resp3-{i}"
        ) for i in range(n)
    ]
    results = await asyncio.gather(*tasks)
    return results


async def bench_aioredis(r: aioredis.Redis, n: int = 1000):
    tasks = [
        asyncio.create_task(
            aioredis_task(i, r), name=f"aioredis-{i}"
        ) for i in range(n)
    ]
    results = await asyncio.gather(*tasks)
    return results


async def aio_task(
        i: int,
        r: aio.AsyncIORedis | aredis.client.StrictRedis | redis.asyncio.client.Redis
) -> int:
    key = f"key:{i}"
    v = await r.get(key)
    new = 1 if v is None else int(v) + 1
    await r.setex(key, 600, new)
    return v


def sio_task(
        i: int,
        r: sio.SyncIORedis | redis.Redis
) -> int:
    key = f"key:{i}"
    v = r.get(key)
    new = 1 if v is None else int(v) + 1
    r.setex(key, 600, new)
    return v


async def aioredis_task(i: int, r: aioredis.Redis) -> int:
    key = f"key:{i}"
    v = await r.get(key)
    new = 1 if v is None else int(v) + 1
    await r.set(key, new, expire=600)
    return v

