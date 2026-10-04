<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Cooperative payload admission with Flight DoExchange

`memory_budget.py` illustrates the application-level workaround discussed in
[GH-37900](https://github.com/apache/arrow/issues/37900#issuecomment-1737277716).
A sender announces the next batch's size and waits for the receiver's grant.
The receiver shares a byte budget across exchanges, consumes granted batches,
and releases each reservation before acknowledging consumption.

This example does **not** implement admission before gRPC allocates memory.
Its budget counts logical int64 value bytes in granted application requests.
It does not cap process RSS, transport read-ahead, schemas, control frames, IPC
metadata, decompression, allocator overhead, or sender allocations. A sender
that ignores the protocol can transmit a large frame before the receiver
validates it. This example assumes cooperating peers.

## Run

Use a Python environment with PyArrow including Flight and pytest installed.
From the repository root:

```sh
python -m pytest -q python/examples/flight/test_memory_budget.py
python python/examples/flight/memory_budget_benchmark.py
```

The benchmark starts both endpoints on localhost and prints JSON. It uses four
clients, a 65,536-byte budget, 32,768-byte batches, and a simulated one-millisecond
consumer delay. Each client attempts 100 batches; one additional oversized batch
must be refused. Counts depend on thread scheduling. These tests live with the
example and must be invoked explicitly.

## Use the example

Run this from `python/examples/flight`, or put that directory on `PYTHONPATH`:

```python
import pyarrow as pa
import pyarrow.flight as flight
from memory_budget import BudgetedFlightServer, ByteBudget, SCHEMA, send_batches

budget = ByteBudget(64 * 1024)

def consume(batch):
    # Finish processing here. Do not retain the batch or its buffers.
    print(batch.num_rows)

with BudgetedFlightServer(("localhost", 0), budget, consume) as server:
    with flight.FlightClient(("localhost", server.port)) as client:
        batch = pa.record_batch([pa.array(range(1024), type=pa.int64())],
                                schema=SCHEMA)
        print(send_batches(client, [batch], timeout=10))
print(budget.snapshot())
```

## Protocol and ownership

The private example command is `example:cooperative-payload-budget:v1`. Control
messages contain a one-byte opcode and an eight-byte unsigned size in network
byte order. Exactly nine metadata bytes are accepted. The fixed schema has one
int64 column named `value`; null values and empty batches are rejected. The
announced size is `rows * 8`. No compression is enabled by the example client.

1. Send `ANNOUNCE(size)` as application metadata, without batch data.
2. The receiver validates the size and tries to reserve it under a shared lock.
   It replies `REFUSE(size)` immediately when the budget is full. The sender
   skips that batch; retry policy belongs to the caller.
3. After `GRANT(size)`, send one batch. The receiver validates its schema and
   logical size, then calls the synchronous consumer while holding the
   reservation. Schema transmission for the first batch is outside accounting.
4. Release the reservation and reply `CONSUMED(size)`. The sender waits for this
   acknowledgement before announcing another batch.

Only one batch can be outstanding per compliant stream. Concurrent consumers
may run on different Flight handler threads, so the supplied callback must be
thread-safe. Budget exhaustion does not queue requests or busy-wait. The default
per-batch limit is 16 MiB of logical values; it is a protocol check performed
after Flight receives each frame, not a transport allocation limit.

Reservations are released on normal consumption, callback failure, invalid
payload, early EOF, cancellation, and deadline errors. The client requires a
finite positive deadline for the complete RPC. An arbitrary peer without a
deadline can hold a grant indefinitely; the server does not enforce a lease.
A consumer that blocks must arrange its own cancellation. A consumer that
retains buffers must extend ownership/accounting itself before using this
pattern in an application.

## Measurement

`reserved_bytes` and `peak_reserved_bytes` describe the logical admission
counter, not allocated memory. The benchmark also reports the combined
client/server process's lifetime peak RSS and Arrow's default memory pool
metrics. RSS is reported on macOS/Linux and is not current usage; Arrow's pool excludes
gRPC allocations.
Both include effects unrelated to the reservation counter. Elapsed time includes
server/client setup, simulated consumption, and shutdown. This workload checks
contention and cleanup; it is not a transport throughput comparison.

For example, vary available capacity while keeping the workload fixed:

```sh
python python/examples/flight/memory_budget_benchmark.py --budget-bytes 65536
python python/examples/flight/memory_budget_benchmark.py --budget-bytes 131072
```

There are no Flight wire-protocol, native allocator, or C++ API changes. Applying
this pattern to downloads would place the same budget and grant decision at the
client receiver. It requires an application protocol agreed by both peers and
does not interoperate automatically with generic Flight clients.
