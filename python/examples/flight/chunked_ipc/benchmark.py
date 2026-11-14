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

"""Reproducible localhost round-trip benchmark; prints one JSON row per run.

RSS is a lifetime process high-water mark covering BOTH client and server.
tracemalloc measures Python allocations during the exchange, not native gRPC
or Arrow allocation. The input table is constructed before measurement. Use
a fresh process per configuration when comparing peak RSS.
"""

import argparse
from dataclasses import asdict
import json
import platform
import sys
import time
import tracemalloc

import pyarrow as pa
import pyarrow.flight as flight

from chunked_ipc import ChunkedEchoServer, Limits, exchange


def peak_rss_bytes():
    if sys.platform not in ("darwin", "linux"):
        return None
    import resource
    value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return value if sys.platform == "darwin" else value * 1024


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--value-mib", type=int, default=16)
    parser.add_argument("--frame-kib", type=int, default=64)
    parser.add_argument("--iterations", type=int, default=3)
    parser.add_argument("--kind", choices=["binary", "string"],
                        default="binary")
    args = parser.parse_args()
    if min(args.value_mib, args.frame_kib, args.iterations) <= 0:
        parser.error("sizes and iteration count must be positive")
    value_bytes = args.value_mib * 1024 * 1024
    frame_bytes = args.frame_kib * 1024
    value = (b"x" if args.kind == "binary" else "x") * value_bytes
    table = pa.table({"value": [value]})
    del value
    limits = Limits(frame_bytes, value_bytes + 1024 * 1024)
    # Leave explicit headroom for Flight/protobuf overhead above the metadata
    # cap. A frame cap is not the size of the entire serialized gRPC message.
    grpc_limit = frame_bytes + 64 * 1024
    grpc_options = [("grpc.max_send_message_length", grpc_limit),
                    ("grpc.max_receive_message_length", grpc_limit)]
    with ChunkedEchoServer(limits=limits) as server:
        with flight.FlightClient(
                ("localhost", server.port),
                generic_options=grpc_options) as client:
            for iteration in range(args.iterations):
                rss_before = peak_rss_bytes()
                arrow_before = pa.total_allocated_bytes()
                tracemalloc.start()
                start = time.perf_counter()
                result = exchange(
                    client, table.schema, iter(table.to_batches()), limits,
                    flight.FlightCallOptions(timeout=120))
                elapsed = time.perf_counter() - start
                python_current, python_peak = tracemalloc.get_traced_memory()
                tracemalloc.stop()
                rss_after = peak_rss_bytes()
                arrow_after = pa.total_allocated_bytes()
                assert result.table.equals(table)
                print(json.dumps({
                    "iteration": iteration,
                    "python": platform.python_version(),
                    "pyarrow": pa.__version__,
                    "platform": platform.platform(),
                    "kind": args.kind,
                    "rows": table.num_rows,
                    "input_arrow_bytes": table.nbytes,
                    "limits": asdict(limits),
                    "grpc_message_limit_bytes": grpc_limit,
                    "sent": asdict(result.sent),
                    "received": asdict(result.received),
                    "elapsed_seconds": elapsed,
                    "roundtrip_ipc_mib_per_second": (
                        2 * result.sent.ipc_bytes / 1024**2 / elapsed),
                    "python_tracemalloc_current_bytes": python_current,
                    "python_tracemalloc_peak_bytes": python_peak,
                    "process_peak_rss_before_bytes": rss_before,
                    "process_peak_rss_after_bytes": rss_after,
                    "arrow_pool_live_before_bytes": arrow_before,
                    "arrow_pool_live_after_bytes": arrow_after,
                    "arrow_pool_lifetime_peak_bytes": (
                        pa.default_memory_pool().max_memory()),
                    "memory_scope": (
                        "combined client/server process; "
                        "input precedes tracing; "
                        "RSS and Arrow peaks are lifetime high-water marks; "
                        "tracemalloc excludes native gRPC/Arrow allocations"),
                }), flush=True)
                del result


if __name__ == "__main__":
    main()
