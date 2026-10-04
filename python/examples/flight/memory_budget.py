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

"""Cooperative payload admission over Flight DoExchange.

This example accounts for logical int64 value bytes admitted by the
application.
It does NOT limit gRPC allocations, process RSS, schema/control messages, IPC
metadata, decompression, or allocator overhead. Both peers must follow the
protocol. Flight delivers data only after the transport has allocated it, so
checking a received batch cannot protect against an uncooperative sender.

A sender announces one nonempty batch, waits for a grant, sends that batch, and
waits for a consumed acknowledgement before announcing the next batch. A shared
receiver budget refuses requests immediately when full; it does not queue them.
The consumer must finish synchronously and must not retain batches or buffers.
Client deadlines bound a stalled cooperative exchange; an arbitrary peer can
hold a reservation indefinitely, and a blocking consumer must implement its own
cancellation. This is an application example, not a standard Flight protocol.
"""

import contextlib
from dataclasses import dataclass
import math
import struct
import threading

import pyarrow as pa
import pyarrow.flight as flight


SCHEMA = pa.schema([("value", pa.int64())])
COMMAND = b"example:cooperative-payload-budget:v1"
ANNOUNCE, GRANT, REFUSE, CONSUMED = range(1, 5)
CONTROL = struct.Struct("!BQ")
MAX_UINT64 = (1 << 64) - 1


def _positive_size(size):
    # bool is an int subclass, but is not a meaningful byte count.
    if type(size) is not int or not 0 < size <= MAX_UINT64:
        raise ValueError("byte count must be a positive uint64 integer")
    return size


@dataclass(frozen=True)
class BudgetStats:
    capacity_bytes: int
    reserved_bytes: int
    peak_reserved_bytes: int
    admitted: int
    refused: int


class ByteBudget:
    """Thread-safe, immediate admission of application payload reservations."""

    def __init__(self, capacity_bytes):
        self._capacity_bytes = _positive_size(capacity_bytes)
        self._lock = threading.Lock()
        self._reserved = self._peak = self._admitted = self._refused = 0

    @property
    def capacity_bytes(self):
        return self._capacity_bytes

    def try_reserve(self, size):
        """Return an owned reservation, or None without waiting when full."""
        _positive_size(size)
        with self._lock:
            if size > self.capacity_bytes - self._reserved:
                self._refused += 1
                return None
            reservation = _Reservation(self, size)
            self._reserved += size
            self._peak = max(self._peak, self._reserved)
            self._admitted += 1
            return reservation

    def snapshot(self):
        with self._lock:
            return BudgetStats(self.capacity_bytes, self._reserved, self._peak,
                               self._admitted, self._refused)


class _Reservation:
    def __init__(self, budget, size):
        self._budget = budget
        self._size = size
        self._released = False

    def release(self):
        """Release once; repeated release is harmless and returns False."""
        with self._budget._lock:
            if self._released:
                return False
            self._budget._reserved -= self._size
            self._released = True
            return True

    def __enter__(self):
        with self._budget._lock:
            if self._released:
                raise RuntimeError("reservation already released")
        return self

    def __exit__(self, *exc):
        self.release()


def control_message(kind, size):
    _positive_size(size)
    if kind not in (ANNOUNCE, GRANT, REFUSE, CONSUMED):
        raise ValueError("unknown control message")
    return CONTROL.pack(kind, size)


def read_control(chunk, allowed):
    """Validate before parsing; Flight has already allocated the frame."""
    metadata = chunk.app_metadata
    if (chunk.data is not None or metadata is None or
            len(metadata) != CONTROL.size):
        raise ValueError("expected a nine-byte metadata-only control message")
    kind, size = CONTROL.unpack(metadata)
    if kind not in allowed:
        raise ValueError("unexpected control message")
    _positive_size(size)
    return kind, size


def payload_size(batch):
    """Logical value bytes for the example's non-null int64 schema only."""
    if (not batch.schema.equals(SCHEMA, check_metadata=True) or
            batch.column(0).null_count):
        raise ValueError("expected one non-null int64 column named value")
    return _positive_size(batch.num_rows * 8)


class BudgetedFlightServer(flight.FlightServerBase):
    """Consume admitted batches and share one budget across all exchanges."""

    def __init__(self, location, budget, consume, *,
                 max_batch_bytes=16 * 1024 * 1024):
        self.budget = budget
        self.consume = consume
        self.max_batch_bytes = _positive_size(max_batch_bytes)
        super().__init__(location)

    def do_exchange(self, context, descriptor, reader, writer):
        if (descriptor.descriptor_type != flight.DescriptorType.CMD or
                descriptor.command != COMMAND):
            raise ValueError("unknown example protocol")
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                return
            _, size = read_control(chunk, {ANNOUNCE})
            if size > self.max_batch_bytes or size % 8:
                raise ValueError("announced payload exceeds the batch limit "
                                 "or is not a multiple of eight")
            reservation = self.budget.try_reserve(size)
            if reservation is None:
                writer.write_metadata(control_message(REFUSE, size))
                continue
            # Covers grant write failure, premature EOF, validation failure,
            # callback exceptions, and transport cancellation/deadline errors.
            with reservation:
                writer.write_metadata(control_message(GRANT, size))
                try:
                    chunk = reader.read_chunk()
                except StopIteration as exc:
                    raise ValueError("stream ended before granted batch") \
                        from exc
                try:
                    if (chunk.data is None or
                            chunk.app_metadata is not None or
                            payload_size(chunk.data) != size):
                        raise ValueError("batch does not match its grant")
                    self.consume(chunk.data)
                finally:
                    # Do not keep the last batch alive while waiting for the
                    # next announcement. Consumers must also drop references.
                    chunk = None
            writer.write_metadata(control_message(CONSUMED, size))


def send_batches(client, batches, *, timeout=10.0):
    """Send cooperatively; return admitted/refused counts without retrying.

    The timeout covers the whole RPC. Callers decide when to retry refused
    batches; this example never silently retries or buffers an unbounded queue.
    Batches are already allocated by the sender before they are announced.
    """
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be finite and positive")
    writer, reader = client.do_exchange(
        flight.FlightDescriptor.for_command(COMMAND),
        options=flight.FlightCallOptions(timeout=timeout))
    admitted = refused = 0
    started = False
    try:
        for batch in batches:
            size = payload_size(batch)
            writer.write_metadata(control_message(ANNOUNCE, size))
            kind, reply_size = read_control(
                reader.read_chunk(), {GRANT, REFUSE})
            if reply_size != size:
                raise ValueError("reply does not match announcement")
            if kind == REFUSE:
                refused += 1
                continue
            if not started:
                writer.begin(SCHEMA)
                started = True
            writer.write_batch(batch)
            _, reply_size = read_control(reader.read_chunk(), {CONSUMED})
            if reply_size != size:
                raise ValueError("acknowledgement does not match batch")
            admitted += 1
        writer.done_writing()
        try:
            reader.read_chunk()
        except StopIteration:
            pass
        else:
            raise ValueError("unexpected message after end of exchange")
        writer.close()
    except BaseException:
        # In particular, cancel if producing/validating a batch failed while
        # the server was waiting. Preserve the original exception on close.
        reader.cancel()
        with contextlib.suppress(Exception):
            writer.close()
        raise
    return {"admitted": admitted, "refused": refused}
