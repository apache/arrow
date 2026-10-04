# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Localhost workload for cooperative Flight payload admission.

The simulated consumer delay keeps reservations occupied to expose contention.
Elapsed time is not a general Flight throughput benchmark. The process contains
both endpoints; RSS includes native transport/Arrow allocations and is not
bounded by the application's logical payload budget.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict
import json
import platform
import sys
import time

import pyarrow as pa
import pyarrow.flight as flight

from memory_budget import (
    BudgetedFlightServer, ByteBudget, SCHEMA, send_batches,
)


def peak_rss_bytes():
    # getrusage reports bytes on macOS and KiB on Linux. Only assign units
    # on those platforms.
    if sys.platform != "darwin" and not sys.platform.startswith("linux"):
        return None
    import resource
    raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    if sys.platform == "darwin":
        return raw
    return raw * 1024


def run_benchmark(*, workers=4, batches=100, rows=4096,
                  budget_bytes=65536, consumer_delay=0.001):
    if min(workers, batches, rows, budget_bytes) <= 0:
        raise ValueError("workload sizes must be positive")
    if max(rows * 8, budget_bytes + 8) > 16 * 1024 * 1024:
        raise ValueError("benchmark payloads must fit the 16 MiB batch limit")
    if not 0 <= consumer_delay <= 1:
        raise ValueError("consumer delay must be between zero and one second")
    budget = ByteBudget(budget_bytes)
    batch = pa.record_batch([pa.array(range(rows), type=pa.int64())],
                            schema=SCHEMA)
    # Deliberately over budget to exercise refusal even with one worker.
    oversized = pa.record_batch(
        [pa.array(range(budget_bytes // 8 + 1), type=pa.int64())],
        schema=SCHEMA)
    pool = pa.default_memory_pool()
    before = {"rss_high_water_bytes": peak_rss_bytes(),
              "arrow_pool_bytes_allocated": pool.bytes_allocated()}

    def consume(batch):
        time.sleep(consumer_delay)

    start = time.perf_counter()
    with BudgetedFlightServer(("localhost", 0), budget, consume) as server:
        def send(_):
            with flight.FlightClient(("localhost", server.port)) as client:
                return send_batches(
                    client, (batch for _ in range(batches)),
                    timeout=max(30, batches * consumer_delay * 4))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            outcomes = list(executor.map(send, range(workers)))
        with flight.FlightClient(("localhost", server.port)) as client:
            outcomes.append(send_batches(client, [oversized]))
    elapsed = time.perf_counter() - start
    stats = budget.snapshot()
    assert stats.reserved_bytes == 0
    assert stats.peak_reserved_bytes <= budget_bytes
    assert stats.admitted == sum(item["admitted"] for item in outcomes)
    assert stats.refused == sum(item["refused"] for item in outcomes)
    return {
        "python": platform.python_version(), "pyarrow": pa.__version__,
        "platform": platform.platform(),
        "workload": {"workers": workers, "batches_per_worker": batches,
                     "rows_per_batch": rows,
                     "payload_bytes_per_batch": rows * 8,
                     "consumer_delay_seconds": consumer_delay,
                     "extra_oversized_attempts": 1},
        "elapsed_seconds": elapsed,
        "budget": asdict(stats),
        "memory_before": before,
        "memory_after": {
            "rss_high_water_bytes": peak_rss_bytes(),
            "arrow_pool_bytes_allocated": pool.bytes_allocated(),
            "arrow_pool_lifetime_peak_bytes": pool.max_memory()},
        "metric_definitions": {
            "reserved_bytes": "Application logical int64 value bytes granted "
                              "but not yet consumed or released on failure.",
            "rss_high_water_bytes": "Lifetime peak RSS of the combined "
                                    "client/server process; not current RSS.",
            "arrow_pool_bytes_allocated": "Current bytes in Arrow's default "
                                          "pool; excludes gRPC allocations.",
            "arrow_pool_lifetime_peak_bytes": "Lifetime high-water allocation "
                                              "in Arrow's default pool.",
            "elapsed_seconds": "Localhost workload including setup, "
                               "simulated consumer delay, and shutdown."},
        "scope": "Cooperative application admission only; no cap on gRPC "
                 "allocation, read-ahead, IPC metadata, schemas, "
                 "decompression, allocator overhead, sender memory, "
                 "or process RSS."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--batches", type=int, default=100)
    parser.add_argument("--rows", type=int, default=4096)
    parser.add_argument("--budget-bytes", type=int, default=65536)
    parser.add_argument("--consumer-delay", type=float, default=0.001)
    print(json.dumps(run_benchmark(**vars(parser.parse_args())), indent=2))
