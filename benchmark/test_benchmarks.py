from __future__ import annotations

import inspect


def test_benchmark(benchmark, bench_target, event_loop):
    name, (r, func) = bench_target
    n = 1000
    benchmark.group = f"Simple Get and Set ({n:,} calls per iteration)"
    benchmark.name = name
    if inspect.iscoroutinefunction(func):
        def run():
            event_loop.run_until_complete(func(r=r, n=n))
    else:
        def run():
            func(r=r, n=n)

    benchmark(run)

