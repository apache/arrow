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

"""Unit and localhost RPC tests for the cooperative admission example.

Run explicitly with: pytest python/examples/flight/test_memory_budget.py
"""

from concurrent.futures import ThreadPoolExecutor
import contextlib
import threading
import time

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from memory_budget import (
    ANNOUNCE, COMMAND, CONSUMED, CONTROL, GRANT, MAX_UINT64, SCHEMA,
    BudgetedFlightServer, ByteBudget, control_message, payload_size,
    read_control, send_batches,
)


def make_batch(rows=8):
    return pa.record_batch([pa.array(range(rows), type=pa.int64())],
                           schema=SCHEMA)


@contextlib.contextmanager
def exchange(client, timeout=5):
    writer, reader = client.do_exchange(
        flight.FlightDescriptor.for_command(COMMAND),
        options=flight.FlightCallOptions(timeout=timeout))
    try:
        yield writer, reader
    finally:
        reader.cancel()
        with contextlib.suppress(pa.ArrowException):
            writer.close()


def wait_for_release(budget):
    deadline = time.monotonic() + 5
    while budget.snapshot().reserved_bytes and time.monotonic() < deadline:
        time.sleep(0.005)
    assert budget.snapshot().reserved_bytes == 0


@pytest.mark.parametrize("size", [0, -1, True, 1.5, "8", MAX_UINT64 + 1])
def test_invalid_sizes(size):
    with pytest.raises(ValueError):
        ByteBudget(size)
    budget = ByteBudget(64)
    with pytest.raises(ValueError):
        budget.try_reserve(size)
    assert budget.snapshot().reserved_bytes == 0


def test_reservation_lifetime():
    budget = ByteBudget(64)
    reservation = budget.try_reserve(64)
    assert budget.try_reserve(1) is None
    assert reservation.release()
    assert not reservation.release()
    with pytest.raises(RuntimeError, match="already released"):
        with reservation:
            pass
    with pytest.raises(RuntimeError, match="consumer failed"):
        with budget.try_reserve(64):
            raise RuntimeError("consumer failed")
    assert budget.snapshot().reserved_bytes == 0
    assert budget.snapshot().peak_reserved_bytes == 64
    with pytest.raises(AttributeError):
        budget.capacity_bytes = 128


def test_concurrent_reservations_do_not_overcommit():
    budget = ByteBudget(64)
    barrier = threading.Barrier(16)

    def reserve():
        reservation = budget.try_reserve(16)
        barrier.wait(timeout=5)
        if reservation:
            reservation.release()
        return reservation is not None

    with ThreadPoolExecutor(max_workers=16) as pool:
        admitted = sum(pool.map(lambda _: reserve(), range(16)))
    stats = budget.snapshot()
    assert admitted == stats.admitted == 4
    assert stats.refused == 12
    assert stats.peak_reserved_bytes == 64
    assert stats.reserved_bytes == 0


@pytest.mark.parametrize("raw", [b"", b"x" * 10, CONTROL.pack(99, 8),
                                 CONTROL.pack(ANNOUNCE, 0)])
def test_control_validation(raw):
    chunk = flight.FlightStreamChunk(None, pa.py_buffer(raw))
    with pytest.raises(ValueError):
        read_control(chunk, {ANNOUNCE})


def test_payload_restrictions():
    assert payload_size(make_batch()) == 64
    for batch in [make_batch(0), pa.record_batch([[None]], schema=SCHEMA),
                  pa.record_batch([[1]], names=["other"]),
                  pa.record_batch([[1.0]], names=["value"])]:
        with pytest.raises(ValueError):
            payload_size(batch)


def test_rpc_accept_refuse_and_reuse():
    budget = ByteBudget(64)
    seen = []

    def consume(batch):
        assert budget.snapshot().reserved_bytes == batch.num_rows * 8
        seen.append(batch.num_rows)

    with BudgetedFlightServer(("localhost", 0), budget, consume) as server, \
            flight.FlightClient(("localhost", server.port)) as client:
        assert send_batches(client, [make_batch(8), make_batch(16),
                                     make_batch(8)]) == {
                                         "admitted": 2, "refused": 1}
        assert send_batches(client, [make_batch(8)]) == {
            "admitted": 1, "refused": 0}
    assert seen == [8, 8, 8]
    stats = budget.snapshot()
    assert stats.admitted == 3
    assert stats.refused == 1
    assert stats.peak_reserved_bytes == 64
    assert stats.reserved_bytes == 0


def test_rpc_concurrent_refusal_while_consumer_owns_batch():
    budget = ByteBudget(64)
    entered = threading.Event()
    finish = threading.Event()

    def consume(batch):
        entered.set()
        assert finish.wait(timeout=5)

    with BudgetedFlightServer(("localhost", 0), budget, consume) as server, \
            flight.FlightClient(("localhost", server.port)) as first, \
            flight.FlightClient(("localhost", server.port)) as second, \
            ThreadPoolExecutor(max_workers=1) as pool:
        pending = pool.submit(send_batches, first, [make_batch()])
        try:
            assert entered.wait(timeout=5)
            assert send_batches(second, [make_batch()]) == {
                "admitted": 0, "refused": 1}
            assert budget.snapshot().reserved_bytes == 64
        finally:
            finish.set()
        assert pending.result(timeout=5)["admitted"] == 1
        assert send_batches(second, [make_batch()])["admitted"] == 1
    assert budget.snapshot().reserved_bytes == 0
    assert budget.snapshot().peak_reserved_bytes == 64


def test_rpc_consumer_exception_releases_reservation():
    budget = ByteBudget(64)
    fail = True

    def consume(batch):
        nonlocal fail
        if fail:
            fail = False
            raise RuntimeError("consumer failed")

    with BudgetedFlightServer(("localhost", 0), budget, consume) as server, \
            flight.FlightClient(("localhost", server.port)) as client:
        with pytest.raises(pa.ArrowException, match="consumer failed"):
            send_batches(client, [make_batch()])
        wait_for_release(budget)
        assert send_batches(client, [make_batch()])["admitted"] == 1


@pytest.mark.parametrize("failure", ["cancel", "deadline", "eof", "size"])
def test_rpc_incomplete_or_mismatched_batch_releases(failure):
    budget = ByteBudget(64)
    with BudgetedFlightServer(("localhost", 0), budget,
                              lambda batch: None) as server, \
            flight.FlightClient(("localhost", server.port)) as client:
        timeout = 0.2 if failure == "deadline" else 5
        with exchange(client, timeout=timeout) as (writer, reader):
            writer.write_metadata(control_message(ANNOUNCE, 64))
            assert read_control(reader.read_chunk(), {GRANT}) == (GRANT, 64)
            assert budget.snapshot().reserved_bytes == 64
            if failure == "cancel":
                reader.cancel()
            elif failure == "deadline":
                with pytest.raises(pa.ArrowException):
                    reader.read_chunk()
            else:
                if failure == "eof":
                    writer.done_writing()
                else:
                    writer.begin(SCHEMA)
                    writer.write_batch(make_batch(4))
                with pytest.raises(pa.ArrowException,
                                   match="before granted|match"):
                    reader.read_chunk()
        wait_for_release(budget)
        assert send_batches(client, [make_batch()])["admitted"] == 1


@pytest.mark.parametrize("raw", [b"x" * 10, CONTROL.pack(ANNOUNCE, 0),
                                 CONTROL.pack(ANNOUNCE, 7),
                                 CONTROL.pack(ANNOUNCE, 128),
                                 CONTROL.pack(CONSUMED, 64)])
def test_rpc_rejects_invalid_announcements(raw):
    budget = ByteBudget(64)
    with BudgetedFlightServer(("localhost", 0), budget, lambda batch: None,
                              max_batch_bytes=64) as server, \
            flight.FlightClient(("localhost", server.port)) as client:
        with exchange(client) as (writer, reader):
            writer.write_metadata(raw)
            with pytest.raises(pa.ArrowException):
                reader.read_chunk()
    assert budget.snapshot().admitted == 0
    assert budget.snapshot().reserved_bytes == 0


def test_rpc_rejects_data_before_announcement():
    budget = ByteBudget(64)
    with BudgetedFlightServer(("localhost", 0), budget,
                              lambda batch: None) as server, \
            flight.FlightClient(("localhost", server.port)) as client:
        with exchange(client) as (writer, reader):
            writer.begin(SCHEMA)
            writer.write_batch(make_batch())
            with pytest.raises(pa.ArrowException, match="control message"):
                reader.read_chunk()
    assert budget.snapshot().admitted == 0


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_client_deadline(timeout):
    with pytest.raises(ValueError, match="timeout"):
        send_batches(None, [], timeout=timeout)
