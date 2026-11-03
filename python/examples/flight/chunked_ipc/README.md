<!--
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

# Experimental chunked IPC over Flight

A single binary or string value can exceed a gRPC message limit. Splitting a
record batch into fewer rows cannot split that value. This example serializes
an Arrow IPC stream incrementally and carries its bytes in bounded,
metadata-only `DoExchange` messages. The server spools the upload to a temporary
file, then echoes the bytes; the client reconstructs and decodes the table.

This is a private application protocol for exploring
[GH-34485](https://github.com/apache/arrow/issues/34485). It does not modify
`Flight.proto`, implement native fragmented `FlightData`, or make ordinary
Flight clients understand the fragments. Both endpoints must opt in.

## Run

Install a Flight-enabled PyArrow and pytest in your development environment.
From the Arrow repository root:

```console
python -m pytest python/examples/flight/chunked_ipc/test_chunked_ipc.py -q
python python/examples/flight/chunked_ipc/benchmark.py --value-mib 16 --frame-kib 64 --iterations 3
```

The tests and benchmark launch a real localhost Flight server on an ephemeral
port. The tests send 4 MiB values in one row while the client's gRPC send and
receive caps are 128 KiB. A control test shows an ordinary Flight write of that
row fails at the same cap. Binary, string, large binary, large string,
dictionary replacement, schema/field metadata and zero-batch streams are
covered. The benchmark accepts `--kind string` and emits JSON lines.

From this directory, a minimal call looks like:

```python
import pyarrow as pa
import pyarrow.flight as flight
from chunked_ipc import ChunkedEchoServer, Limits, exchange

table = pa.table({"value": [b"x" * (4 * 1024 * 1024)]})
limits = Limits(max_frame_bytes=16 * 1024, max_total_bytes=8 * 1024 * 1024)
with ChunkedEchoServer(limits=limits) as server:
    with flight.FlightClient(("localhost", server.port)) as client:
        result = exchange(client, table.schema, iter(table.to_batches()),
                          limits, flight.FlightCallOptions(timeout=30))
        assert result.table.equals(table)
        print(result.sent, result.received)
```

The `batches` argument can be a generator. The sender asks for the next batch
after writing the previous batch, and closes the iterator after sending starts
if it exposes `close()`. A huge input value still has to exist in the source
batch; this is fragmentation of its serialized representation.

## Example protocol

1. `DoAction("arrow.example.chunked-ipc.v1.capabilities")` returns the protocol
   name and server limits. The client takes the smaller local/server limits.
2. A command descriptor contains the protocol name, newline, and a JSON object
   with the selected `max_frame_bytes` and `max_total_bytes`. The server checks
   that both are within its policy before creating a temporary file.
3. Each `FlightData.app_metadata` contains a 21-byte header followed by payload.
   No outer Flight schema or record batches are sent.
4. The client sends the IPC stream and an explicit `END`, then half-closes the
   upload. Only after a clean incoming EOF does the server replay the stream.
5. The client validates response framing, requires a clean EOF, then decodes.

| Header field | Encoding | Meaning |
| --- | --- | --- |
| Magic | 4 bytes, `CIP1` | Versioned example frame |
| Kind | 1 byte | `0` = DATA; `1` = END |
| Sequence | unsigned 64-bit, big endian | Starts at zero; increments per frame |
| Offset | unsigned 64-bit, big endian | Number of preceding IPC payload bytes |

DATA payloads must be nonempty. END has no payload and its offset must equal
the received byte count. A frame's entire metadata field, including header,
must fit the selected frame cap. This is not the size of the enclosing protobuf
or gRPC message; transport configuration must leave overhead headroom.

Out-of-order/duplicate frames, offset gaps/overlaps, unknown versions/kinds,
oversized frames, excessive total bytes, missing END, and frames after END are
rejected. A modern IPC end marker is also required. Decoding rejects trailing
bytes after the IPC stream. The echo server treats the IPC body as opaque;
semantic IPC validation occurs at the client. Framing has no checksum or
resume support and relies on the underlying reliable stream for delivery.

## Memory, disk and copying

The bounds are **per exchange**, not a process-wide admission policy:

* The receiver's temporary file contains at most `max_total_bytes` encoded IPC
  payload bytes. It does not allocate a Python buffer from an advertised total
  length or retain a list of fragments. The default total cap is 256 MiB.
* Framing scratch space is proportional to `max_frame_bytes`: a sender copies
  each payload slice into a header-plus-payload message, and a receiver borrows
  an incoming metadata view while writing it to the file. Replay reads one
  payload chunk at a time. Python file buffering and object overhead also exist.
* This accounting excludes the source batch, IPC/Python bridge buffers, incoming
  Flight message allocations, queued native gRPC writes, OS file cache and the
  complete decoded result. Returning a table requires memory proportional to
  its contents. Native IPC/gRPC allocations are not bounded by these checks;
  an oversized message reaches the transport before Python can reject it.
* The sender uses uncompressed IPC. The decoder is the ordinary IPC reader and
  does not enforce a decompressed-allocation cap against hostile IPC input.
  Use this example with trusted peers. Disk-full and decoding failures propagate
  while context managers close temporary files.

This trades zero-copy transfer for small messages and disk-backed staging.
Each direction copies frame payloads and writes/reads a temporary file. The
server waits for the complete upload before responding, so it demonstrates
bounded-message transport, not a low-latency pipelined service. Concurrent
exchanges can each consume their full cap. A deployment would need disk quotas,
admission control, authentication, deadlines and an agreed protocol.

One IPC writer/reader spans the complete transfer. Schema metadata, dictionary
messages, dictionary replacements and record-batch boundaries remain inside
that stream; fragment boundaries can cut through any of them. A fragment is
not an independently readable Arrow record batch.

## Negotiation, fallback and cancellation

An unsupported capability action fails before consuming source batches. There
is no silent fallback: a caller may explicitly use normal Flight with smaller
batches where row slicing is sufficient, or report that a single value exceeds
the transport limit. A native Flight implementation would need a separately
agreed capability and wire format. Capability discovery here is application
convention, not authentication or a standardized Flight feature.

On source or receive failure, the client cancels the reader and closes the
writer. Server and client reassembly files use context managers; cancellation,
deadline expiration, malformed input and normal completion release them. Tests
cover cancellation and timeout while the server is blocked reading a partial
upload. A process crash and arbitrary user generators blocking outside Flight
are outside this cleanup guarantee.

## Interpreting the benchmark

The reported IPC bytes and metadata frame counts are measured, but exclude
protobuf envelopes, descriptors, capability messages and transport framing.
The input table is created before timing/tracing. Timing covers capability
discovery, upload, server staging, download and decoding, with both peers in
one process. Equality validation runs after timing.

`tracemalloc` reports Python allocations during that phase; it does not measure
native Arrow/gRPC allocations. Arrow's pool counters do not include Python
buffers that may back decoded arrays. RSS is the process **lifetime high-water
mark** and includes both peers, libraries, allocator retention and input. It is
not per-call retained memory or a gRPC-only peak. Run each configuration in a
fresh process for comparisons; later iterations share caches and high-water
marks. These local measurements establish feasibility, not production speed
or zero-copy equivalence.
