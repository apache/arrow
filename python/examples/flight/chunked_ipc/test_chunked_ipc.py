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

"""Pure framing tests and real localhost Flight RPC integration tests."""

import contextlib
import dataclasses
import threading

import pyarrow as pa
import pyarrow.flight as flight
import pytest

import chunked_ipc as example


def frame(kind=example.DATA, sequence=0, offset=0, payload=b"x",
          magic=example.MAGIC):
    return example.HEADER.pack(magic, kind, sequence, offset) + payload


def encoded(table, limits=None):
    frames = []
    stats = example.send_batches(
        frames.append, table.schema, table.to_batches(),
        limits or example.Limits())
    return frames, stats


def decode(frames, limits):
    with example.Reassembler(limits) as receiver:
        for metadata in frames:
            receiver.accept(metadata)
        receiver.finish()
        return receiver.read_table()


@pytest.mark.parametrize("kwargs", [
    {"max_frame_bytes": 0}, {"max_frame_bytes": example.HEADER.size},
    {"max_frame_bytes": True}, {"max_total_bytes": -1},
    {"max_total_bytes": 2**63}, {"max_total_bytes": 1.5},
])
def test_invalid_limits(kwargs):
    with pytest.raises(example.ProtocolError):
        example.Limits(**kwargs)


def test_exact_frame_and_total_bounds():
    table = pa.table({"value": [b"x" * 10000]})
    frames, initial = encoded(table, example.Limits(128, 20000))
    limits = example.Limits(128, initial.ipc_bytes)
    frames, stats = encoded(table, limits)
    assert max(map(len, frames)) == limits.max_frame_bytes
    assert stats.metadata_bytes == sum(map(len, frames))
    assert stats.ipc_bytes == stats.metadata_bytes - len(frames) * 21
    assert stats.frames == len(frames)
    assert decode(frames, limits).equals(table)
    with pytest.raises(example.ProtocolError, match="total byte limit"):
        encoded(table, example.Limits(128, initial.ipc_bytes - 1))
    with pytest.raises(example.ProtocolError, match="total byte limit"):
        decode(frames, example.Limits(128, initial.ipc_bytes - 1))


@pytest.mark.parametrize("frames, message", [
    ([b"short"], "truncated frame header"),
    ([frame(magic=b"BAD!")], "magic or kind"),
    ([frame(kind=2)], "magic or kind"),
    ([frame(sequence=1)], "out-of-order"),
    ([frame(offset=1)], "byte offset"),
    ([frame(payload=b"")], "DATA must contain"),
    ([frame(payload=b"x" * 200)], "frame exceeds"),
    ([frame(), frame()], "out-of-order"),
    ([frame(), frame(sequence=1, offset=0)], "byte offset"),
    ([frame(), frame(sequence=2, offset=1)], "out-of-order"),
    ([frame(kind=example.END)], "END must not contain"),
    ([frame(kind=example.END, payload=b""), frame()], "frame after END"),
])
def test_malformed_frames(frames, message):
    with example.Reassembler(example.Limits(128, 10000)) as receiver:
        with pytest.raises(example.ProtocolError, match=message):
            for metadata in frames:
                receiver.accept(metadata)
    assert receiver.file.closed


def test_truncated_flight_and_ipc_streams():
    table = pa.table({"value": [1, 2]})
    frames, _ = encoded(table)
    with pytest.raises(example.ProtocolError, match="missing END"):
        decode(frames[:-1], example.Limits())
    with pytest.raises(example.ProtocolError, match="truncated IPC"):
        decode([frame(kind=example.END, payload=b"")], example.Limits())
    raw = b"".join(f[example.HEADER.size:] for f in frames[:-1])
    # A syntactically framed END is insufficient if IPC itself was truncated.
    for damaged in (raw[:-8], b"not an IPC stream" + example.IPC_EOS,
                    raw + example.IPC_EOS):
        frames = [frame(payload=damaged),
                  frame(kind=example.END, sequence=1,
                        offset=len(damaged), payload=b"")]
        with pytest.raises((example.ProtocolError, pa.ArrowInvalid,
                            pa.ArrowIOError)):
            decode(frames, example.Limits())


def test_no_decode_or_replay_before_eof():
    frames, _ = encoded(pa.table({"v": [1]}))
    with example.Reassembler(example.Limits()) as receiver:
        for metadata in frames:
            receiver.accept(metadata)
        with pytest.raises(example.ProtocolError, match="clean EOF"):
            receiver.read_table()
        with pytest.raises(example.ProtocolError, match="clean EOF"):
            receiver.replay(lambda metadata: None)


def test_sink_rejects_writes_after_end():
    with example.FrameSink(lambda metadata: None, example.Limits()) as sink:
        sink.finish()
        with pytest.raises(example.ProtocolError):
            sink.write(b"x")
        with pytest.raises(example.ProtocolError):
            sink.finish()


def test_lazy_batches_and_source_cleanup_on_send_failure():
    events = []
    schema = pa.schema([("v", pa.int64())])

    def batches():
        try:
            events.append("first")
            yield pa.record_batch([[1]], schema=schema)
            # The previous batch was sent before the next batch was requested.
            assert "send" in events
            events.append("second")
            yield pa.record_batch([[2]], schema=schema)
        finally:
            events.append("closed")

    frames = []

    def send(metadata):
        events.append("send")
        frames.append(metadata)

    example.send_batches(send, schema, batches(), example.Limits())
    assert events.count("closed") == 1
    assert decode(frames, example.Limits()).column(0).to_pylist() == [1, 2]

    events.clear()

    def fail(metadata):
        raise RuntimeError("send failed")

    with pytest.raises(RuntimeError, match="send failed"):
        example.send_batches(fail, schema, batches(), example.Limits())
    assert events[-1] == "closed"


def client_for(server):
    return flight.FlightClient(
        ("localhost", server.port), generic_options=[
            ("grpc.max_send_message_length", 128 * 1024),
            ("grpc.max_receive_message_length", 128 * 1024)])


@pytest.mark.parametrize("arrow_type, value", [
    (pa.binary(), b"x" * (4 * 1024 * 1024)),
    (pa.string(), "y" * (4 * 1024 * 1024)),
    (pa.large_binary(), b"z" * (4 * 1024 * 1024)),
    (pa.large_string(), "w" * (4 * 1024 * 1024)),
])
def test_large_single_row_over_real_rpc(arrow_type, value):
    table = pa.table({"value": pa.array([value], type=arrow_type)})
    # Slicing cannot shrink the one value; fragment its encoded bytes instead.
    assert table.slice(0, 1).nbytes > 128 * 1024
    limits = example.Limits(16 * 1024, 8 * 1024 * 1024)
    with example.ChunkedEchoServer(limits=limits) as server:
        with client_for(server) as client:
            result = example.exchange(
                client, table.schema, iter(table.to_batches()), limits,
                flight.FlightCallOptions(timeout=20))
    assert result.table.equals(table, check_metadata=True)
    assert result.sent.ipc_bytes == result.received.ipc_bytes
    assert result.sent.frames > 256
    assert result.sent.largest_frame_bytes <= limits.max_frame_bytes
    assert result.received.largest_frame_bytes <= limits.max_frame_bytes


class PlainEchoServer(flight.FlightServerBase):
    def do_exchange(self, context, descriptor, reader, writer):
        writer.begin(reader.schema)
        for chunk in reader:
            writer.write_batch(chunk.data)


def test_ordinary_single_row_write_hits_transport_limit():
    batch = pa.record_batch([pa.array([b"x" * (4 * 1024 * 1024)])],
                            names=["value"])
    with PlainEchoServer() as server, client_for(server) as client:
        writer, reader = client.do_exchange(
            flight.FlightDescriptor.for_command(b"echo"),
            options=flight.FlightCallOptions(timeout=10))
        try:
            with pytest.raises((pa.ArrowInvalid, flight.FlightError),
                               match="[Ll]arger than max|[Ee]xceeds|[Ss]ize"):
                writer.begin(batch.schema)
                writer.write_batch(batch)
                writer.done_writing()
                reader.read_all()
        finally:
            reader.cancel()
            with contextlib.suppress(Exception):
                writer.close()


def test_schema_dictionary_replacements_and_empty_table():
    dtype = pa.dictionary(pa.int8(), pa.string())
    schema = pa.schema([pa.field("value", dtype, metadata={b"field": b"yes"})],
                       metadata={b"schema": b"preserved"})
    batches = [pa.record_batch([pa.array(values, type=dtype)], schema=schema)
               for values in (["a", "b", None], ["b", "c"])]
    with example.ChunkedEchoServer() as server, client_for(server) as client:
        result = example.exchange(client, schema, iter(batches),
                                  example.Limits(1024, 100000))
        assert result.table.equals(pa.Table.from_batches(batches),
                                   check_metadata=True)
        empty = example.exchange(client, schema, iter(()),
                                 example.Limits(1024, 100000))
        assert empty.table.num_rows == 0
        assert empty.table.schema.equals(schema, check_metadata=True)


def test_negotiation_uses_smaller_limits():
    server_limits = example.Limits(2048, 8192)
    requested = example.Limits(1024, 16384)
    table = pa.table({"value": [b"x" * 4000]})
    with example.ChunkedEchoServer(limits=server_limits) as server:
        with client_for(server) as client:
            result = example.exchange(client, table.schema, table.to_batches(),
                                      requested)
    assert result.limits == example.Limits(1024, 8192)
    assert result.received.largest_frame_bytes <= 1024


def test_unsupported_server_fails_before_source_consumption():
    consumed = []

    def batches():
        consumed.append(True)
        yield pa.record_batch([[1]], names=["v"])

    with flight.FlightServerBase() as server, client_for(server) as client:
        with pytest.raises(example.ProtocolError, match="does not support"):
            example.exchange(client, pa.schema([("v", pa.int64())]), batches())
    assert not consumed


def tracked_tempfiles(monkeypatch):
    files = []
    created = threading.Event()
    original = example.tempfile.TemporaryFile

    def factory(*args, **kwargs):
        file = original(*args, **kwargs)
        files.append(file)
        created.set()
        return file

    monkeypatch.setattr(example.tempfile, "TemporaryFile", factory)
    return files, created


def test_cancel_partial_rpc_closes_server_spool(monkeypatch):
    files, created = tracked_tempfiles(monkeypatch)
    with example.ChunkedEchoServer() as server, client_for(server) as client:
        writer, reader = client.do_exchange(
            example.exchange_descriptor(example.Limits()),
            options=flight.FlightCallOptions(timeout=10))
        writer.write_metadata(frame(payload=b"partial IPC bytes"))
        assert created.wait(5)
        reader.cancel()
        with contextlib.suppress(Exception):
            writer.close()
    assert files and all(file.closed for file in files)


def test_rpc_truncation_and_source_failure_close_spools(monkeypatch):
    files, _ = tracked_tempfiles(monkeypatch)
    table = pa.table({"value": [1, 2]})
    frames, _ = encoded(table)
    with example.ChunkedEchoServer() as server, client_for(server) as client:
        writer, reader = client.do_exchange(
            example.exchange_descriptor(example.Limits()),
            options=flight.FlightCallOptions(timeout=10))
        try:
            for metadata in frames[:-1]:
                writer.write_metadata(metadata)
            writer.done_writing()
            with pytest.raises(pa.ArrowInvalid, match="missing END"):
                list(reader)
        finally:
            reader.cancel()
            with contextlib.suppress(Exception):
                writer.close()

        def broken_source():
            yield from table.to_batches()
            raise RuntimeError("source failed")

        with pytest.raises(RuntimeError, match="source failed"):
            example.exchange(client, table.schema, broken_source(),
                             options=flight.FlightCallOptions(timeout=10))
    assert files and all(file.closed for file in files)


@pytest.mark.parametrize("metadata, message", [
    (frame(sequence=1), "out-of-order"),
    (frame(offset=100), "byte offset"),
    (frame(payload=b"x" * 2048), "frame exceeds"),
])
def test_rpc_rejects_bad_frames(monkeypatch, metadata, message):
    files, _ = tracked_tempfiles(monkeypatch)
    limits = example.Limits(1024, 4096)
    with example.ChunkedEchoServer(limits=limits) as server:
        with client_for(server) as client:
            writer, reader = client.do_exchange(
                example.exchange_descriptor(limits),
                options=flight.FlightCallOptions(timeout=10))
            try:
                writer.write_metadata(metadata)
                writer.done_writing()
                with pytest.raises(pa.ArrowInvalid, match=message):
                    list(reader)
            finally:
                reader.cancel()
                with contextlib.suppress(Exception):
                    writer.close()
    assert files and all(file.closed for file in files)


def test_rpc_timeout_closes_server_spool(monkeypatch):
    files, created = tracked_tempfiles(monkeypatch)
    with example.ChunkedEchoServer() as server, client_for(server) as client:
        writer, reader = client.do_exchange(
            example.exchange_descriptor(example.Limits()),
            options=flight.FlightCallOptions(timeout=0.5))
        try:
            writer.write_metadata(frame(payload=b"incomplete"))
            assert created.wait(5)
            # Leave the upload open. The server must release its temporary file
            # when the client deadline cancels a blocked incoming read.
            with pytest.raises(flight.FlightTimedOutError):
                reader.read_chunk()
        finally:
            reader.cancel()
            with contextlib.suppress(Exception):
                writer.close()
    assert files and all(file.closed for file in files)


@pytest.mark.parametrize("change", [
    {"max_frame_bytes": 1}, {"max_frame_bytes": True},
    {"max_total_bytes": 0}, {"unexpected": 1},
])
def test_capability_limits_are_validated(change):
    limits = dataclasses.asdict(example.Limits())
    limits.update(change)
    with pytest.raises(example.ProtocolError):
        example._parse_limits(limits)
