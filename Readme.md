# Sans-IO Redis

## What is this?

This repository was inspired by [Sans-IO Python](https://sans-io.readthedocs.io/) as 
an attempt to implement an event-based backed for reading and writing bytes which 
conform to the Redis Client/Server Protocol.

As our community evolves to work with both varied IO implementations, we need to 
re-approach our client libraries in a way which will ease the maintenance burden for 
duel support. This respository is meant to serve as a starting point for creating a 
more robust, maintainable Python client for Redis.

## Organization

The core business logic of the library is held within the `sansio` package. All 
configuration, error modes, core connection operations, command encoding and 
response decoding is contained within this module, which is intentionally free of 
any IO-specific logic.

There are two more top-level packages: `io` and `client`.

The `io` package contains IO-specific implementations of connections and connection 
pools over TCP and UFD sockets for asyncio and standard sync io. These 
implementations make use of the high-level `SansIORedisProtocol` to maintain the 
query/response lifecycle.

The `client` package is similarly organized to the `io` module, but provides a 
partial implementation of a high-level client which mirrors the interface found in 
the popular redis/redis-py library.


## Not Implemented

`MONITOR` and `PUB/SUB` are not yet implemented, as these require a custom 
connection protocol which opens a stream with a single command then continually 
receives responses from the server.

Additionally, only the core redis commands are implemented reliably.


## Benchmarks

A naive benchmark (below) places this implementation's async client within punching 
range of aioredis@v1.3, which was an extremely efficient implementation:

**Async RESP3 w/ 1,000 queries:** 0.061727046966552734 

**aioredis@v1.3 w/ 1,000 queries:** 0.03411412239074707 

**Sync RESP3 w/ 1,000 queries:** 0.22302007675170898

```python
import time
import asyncio

import aioredis
import uvloop

import redis.client.aio
import redis.client.sio
from redis.io import aio, sio
from redis.sansio import protocol


async def task(i, redis: redis.client.aio.AsyncIORedis):
    key = f"key:{i}"
    v = await redis.get(key)
    new = 1 if v is None else int(v) + 1
    await redis.setex(key, 600, new)


def stask(i, redis: redis.client.sio.SyncIORedis):
    key = f"key:{i}"
    v = redis.get(key)
    new = 1 if v is None else int(v) + 1
    redis.setex(key, 600, new)


async def aioredis_task(i, redis: aioredis.Redis):
    key = f"key:{i}"
    v = await redis.get(key)
    new = 1 if v is None else int(v) + 1
    await redis.set(key, new, expire=600)


async def atimeit(n: int = 100):
    proto = protocol.SansIORedisProtocol(
        pool_info=protocol.PoolInfo(
            min_connections=20, max_connections=100
        ),
        client_info=protocol.ClientInfo(
            resp_version="3",
            server_version="6.2"
        )
    )
    async with aio.AsyncIORedisConnectionPool(protocol=proto) as resp3:
        async with redis.client.aio.AsyncIORedis(connection_pool=resp3) as r:
            tasks = [
                asyncio.create_task(
                    task(i, r), name=f"resp3-{i}"
                ) for i in range(n)
            ]
            start = time.time()
            await asyncio.gather(*tasks)
            stop = time.time()
            print(f"Async RESP3 w/ {n:,} queries: {stop - start}")

    aior = await aioredis.create_redis_pool(
        "redis://localhost:6379", encoding='utf-8', maxsize=64
    )
    tasks = [
        asyncio.create_task(
            aioredis_task(i, aior), name=f"aioredis-{i}"
        ) for i in range(n)
    ]
    start = time.time()
    await asyncio.gather(*tasks)
    stop = time.time()
    print(f"aioredis@v1.3 w/ {n:,} queries: {stop - start}")
    aior.close()
    await aior.wait_closed()


def stimeit(n: int = 100):
    proto = protocol.SansIORedisProtocol(
        pool_info=protocol.PoolInfo(
            min_connections=20, max_connections=100
        ),
        client_info=protocol.ClientInfo(
            resp_version="3",
            server_version="6.2"
        )
    )
    with sio.SyncIORedisConnectionPool(protocol=proto) as sresp3:
        with redis.client.sio.SyncIORedis(connection_pool=sresp3) as r:
            start = time.time()
            for i in range(n):
                stask(i, r)
            stop = time.time()
            print(f"Sync RESP3 w/ {n:,} queries: {stop - start}")


if __name__ == '__main__':
    uvloop.install()
    n = 1000
    asyncio.run(atimeit(1000))
    stimeit(1000)

```

