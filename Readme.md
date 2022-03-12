# Sans-IO Redis

## What is this?

This repository was inspired by [Sans-IO Python](https://sans-io.readthedocs.io/) as 
an attempt to implement an event-based backend for reading and writing bytes which 
conform to the Redis Client/Server Protocol.

As our community evolves to work with varied IO implementations, we need to 
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

### Table

|  **Name (time in ms)**   |     **Min**     |     **Max**     |    **Mean**     |   **StdDev**    |   **Median**    |     **IQR**     | **Outliers** |    **OPS**     | **Rounds** | **Iterations** |
|:------------------------:|:---------------:|:---------------:|:---------------:|:---------------:|:---------------:|:---------------:|:------------:|:--------------:|:----------:|:--------------:|
|     aioredis-single      |  50.5607 (1.0)  |  70.4961 (1.0)  |  56.6137 (1.0)  |  5.7495 (2.51)  |  53.9943 (1.0)  |  5.2130 (2.51)  |     6;1      | 17.6636 (1.0)  |     18     |       1        |
|      aioredis-pool       | 62.3477 (1.23)  | 107.8162 (1.53) | 69.4600 (1.23)  | 11.9217 (5.20)  | 66.2642 (1.23)  |  5.8016 (2.80)  |     1;1      | 14.3968 (0.82) |     13     |       1        |
| sansredis-asyncio-single | 84.9354 (1.68)  | 140.8810 (2.00) | 99.2474 (1.75)  | 21.5618 (9.40)  | 88.0335 (1.63)  | 25.1225 (12.11) |     3;0      | 10.0758 (0.57) |     12     |       1        |
|       redis-single       | 95.7767 (1.89)  | 105.0375 (1.49) | 99.6812 (1.76)  |  3.3202 (1.45)  | 98.7644 (1.83)  |  6.1141 (2.95)  |     4;0      | 10.0320 (0.57) |     10     |       1        |
|  sansredis-asyncio-pool  | 96.8407 (1.92)  | 147.2347 (2.09) | 110.1561 (1.95) | 20.0378 (8.73)  | 100.5382 (1.86) | 13.7257 (6.61)  |     2;2      | 9.0780 (0.51)  |     10     |       1        |
|       aredis-pool        | 121.3291 (2.40) | 161.7552 (2.29) | 133.0315 (2.35) | 16.3522 (7.13)  | 126.8798 (2.35) | 13.5703 (6.54)  |     1;1      | 7.5170 (0.43)  |     5      |       1        |
|        redis-pool        | 132.5473 (2.62) | 139.9579 (1.99) | 135.8542 (2.40) |  2.2941 (1.0)   | 135.9680 (2.52) |  2.0750 (1.0)   |     2;1      | 7.3608 (0.42)  |     7      |       1        |
|   redis-asyncio-single   | 177.5305 (3.51) | 243.9758 (3.46) | 205.2752 (3.63) | 29.8083 (12.99) | 188.6406 (3.49) | 50.0484 (24.12) |     1;0      | 4.8715 (0.28)  |     5      |       1        |
| sansredis-syncio-single  | 202.9711 (4.01) | 213.7868 (3.03) | 208.8613 (3.69) |  3.9114 (1.71)  | 209.0654 (3.87) |  4.1701 (2.01)  |     2;0      | 4.7879 (0.27)  |     5      |       1        |
|  sansredis-syncio-pool   | 214.4722 (4.24) | 223.8135 (3.17) | 219.8581 (3.88) |  4.1799 (1.82)  | 220.8717 (4.09) |  7.5444 (3.64)  |     1;0      | 4.5484 (0.26)  |     5      |       1        |
|    redis-asyncio-pool    | 252.1265 (4.99) | 313.8847 (4.45) | 288.0203 (5.09) | 32.1534 (14.02) | 307.9763 (5.70) | 59.5624 (28.71) |     2;0      | 3.4720 (0.20)  |     5      |       1        |


### Histogram

![Benchmark](benchmark-latest.svg)
