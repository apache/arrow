# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0

"""Benchmark: bind_function + FunctionExecutor vs repeated is_in calls.

Reproduces the scenario from issue #49214: a large value_set queried
against many small input arrays, as happens in Ray Data checkpointing
(100M IDs × 20 blocks).
"""

import time
import pyarrow as pa
import pyarrow.compute as pc
import numpy as np


def benchmark_repeated_is_in(value_set_size, num_arrays, array_size, seed=0):
    """Baseline: call pc.is_in() repeatedly, rebuilding MemoTable each time."""
    rng = np.random.default_rng(seed)
    value_set = pa.array(rng.choice(1_000_000, size=value_set_size).tolist())
    arrays = [pa.array(rng.choice(1_000_000, size=array_size).tolist())
              for _ in range(num_arrays)]

    for arr in arrays[:2]:
        pc.is_in(arr, value_set=value_set)

    start = time.perf_counter()
    for arr in arrays:
        pc.is_in(arr, value_set=value_set)
    return time.perf_counter() - start


def benchmark_executor(value_set_size, num_arrays, array_size, seed=0):
    """New path: bind once, execute many - MemoTable built once."""
    rng = np.random.default_rng(seed)
    value_set = pa.array(rng.choice(1_000_000, size=value_set_size).tolist())
    arrays = [pa.array(rng.choice(1_000_000, size=array_size).tolist())
              for _ in range(num_arrays)]

    executor = pc.bind_function(
        "is_in", [arrays[0]],
        options=pc.SetLookupOptions(value_set))

    for arr in arrays[:2]:
        executor.execute([arr])

    start = time.perf_counter()
    for arr in arrays:
        executor.execute([arr])
    return time.perf_counter() - start


def main():
    scenarios = [
        (10_000, 50, 1_000,  "small"),
        (100_000, 200, 5_000, "medium"),
        (500_000, 500, 10_000, "large"),
    ]
    print(f"{'scenario':>10}  {'repeated':>12}  {'executor':>12}  {'speedup':>10}")
    print("-" * 55)
    for vs_size, n_arr, arr_size, label in scenarios:
        baseline = benchmark_repeated_is_in(vs_size, n_arr, arr_size)
        try:
            improved = benchmark_executor(vs_size, n_arr, arr_size)
            speedup = baseline / improved if improved > 0 else float("inf")
            print(f"{label:>10}  {baseline*1000:>10.1f} ms  "
                  f"{improved*1000:>10.1f} ms  {speedup:>8.2f}x")
        except AttributeError:
            print(f"{label:>10}  {baseline*1000:>10.1f} ms  "
                  f"{'N/A':>12}  {'(bind_function not yet built)':>10}")


if __name__ == "__main__":
    main()
