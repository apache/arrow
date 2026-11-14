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

"""Experimental IPC fragmentation over metadata-only Flight DoExchange.

This is an application protocol, not a change to Flight or its protobuf schema.
Both peers must opt in. Reassembly uses a temporary file; decoded Arrow data,
source batches, native IPC/gRPC allocations and OS file cache are not covered
by the bounded frame scratch space. See README.md for the protocol and limits.
"""

import contextlib
from dataclasses import asdict, dataclass
import io
import json
import struct
import tempfile

import pyarrow as pa
import pyarrow.flight as flight


PROTOCOL = "arrow.example.chunked-ipc.v1"
CAPABILITIES_ACTION = PROTOCOL + ".capabilities"
MAGIC = b"CIP1"
DATA = 0
END = 1
HEADER = struct.Struct("!4sBQQ")  # magic, kind, sequence, byte offset
IPC_EOS = b"\xff\xff\xff\xff\x00\x00\x00\x00"


class ProtocolError(ValueError):
    """An invalid or unsupported example protocol message."""


@dataclass(frozen=True)
class Limits:
    """Limits apply to metadata bytes and encoded IPC bytes, respectively."""

    max_frame_bytes: int = 64 * 1024
    max_total_bytes: int = 256 * 1024 * 1024

    def __post_init__(self):
        for value in (self.max_frame_bytes, self.max_total_bytes):
            if type(value) is not int or value <= 0 or value >= 2**63:
                raise ProtocolError("limits must be positive 63-bit integers")
        if self.max_frame_bytes <= HEADER.size:
            raise ProtocolError("frame limit must leave room for a payload")


@dataclass
class Stats:
    ipc_bytes: int = 0
    metadata_bytes: int = 0
    frames: int = 0
    largest_frame_bytes: int = 0

    def add(self, frame_bytes, payload_bytes):
        self.ipc_bytes += payload_bytes
        self.metadata_bytes += frame_bytes
        self.frames += 1
        self.largest_frame_bytes = max(self.largest_frame_bytes, frame_bytes)


class FrameSink(io.RawIOBase):
    """Serialize one bounded metadata frame at a time, without a frame list.

    The callback must consume a frame synchronously. Flight may make its own
    copies or queue writes; this object does not control transport allocation.
    """

    def __init__(self, send_metadata, limits):
        super().__init__()
        self.send_metadata = send_metadata
        self.limits = limits
        self.stats = Stats()
        self.finished = False

    def writable(self):
        return True

    def write(self, data):
        if self.closed or self.finished:
            raise ProtocolError("write after end of stream")
        view = memoryview(data).cast("B")
        if self.stats.ipc_bytes + len(view) > self.limits.max_total_bytes:
            raise ProtocolError("encoded IPC stream exceeds total byte limit")
        payload_limit = self.limits.max_frame_bytes - HEADER.size
        for start in range(0, len(view), payload_limit):
            part = view[start:start + payload_limit]
            header = HEADER.pack(MAGIC, DATA, self.stats.frames,
                                 self.stats.ipc_bytes)
            frame = header + part.tobytes()
            self.send_metadata(frame)
            self.stats.add(len(frame), len(part))
        return len(view)

    def finish(self):
        if self.closed or self.finished:
            raise ProtocolError("duplicate end of stream")
        frame = HEADER.pack(MAGIC, END, self.stats.frames,
                            self.stats.ipc_bytes)
        self.send_metadata(frame)
        self.stats.add(len(frame), 0)
        self.finished = True


def send_batches(send_metadata, schema, batches, limits):
    """Write a single uncompressed IPC stream from a lazy batch iterable.

    The iterable is owned for this call and closed on failure or completion
    when it exposes close(). An input batch, including one huge row, is still
    held in full by the caller/IPC writer while its bytes are fragmented.
    """
    batches = iter(batches)
    try:
        with FrameSink(send_metadata, limits) as sink:
            options = pa.ipc.IpcWriteOptions(
                use_legacy_format=False, compression=None)
            with pa.ipc.new_stream(sink, schema, options=options) as writer:
                for batch in batches:
                    writer.write_batch(batch)
            sink.finish()
            return sink.stats
    finally:
        close = getattr(batches, "close", None)
        if close is not None:
            close()


class Reassembler:
    """Validate framing and spool a capped encoded stream to a temporary file.

    No advertised size causes an allocation. accept() holds a borrowed frame
    view while writing it to disk. IPC decoding can allocate memory
    proportional to the complete output, separately in read_table().
    """

    def __init__(self, limits, temp_dir=None):
        self.limits = limits
        self.stats = Stats()
        self.ended = False
        self.complete = False
        self.file = tempfile.TemporaryFile(dir=temp_dir)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.file.close()

    def accept(self, metadata):
        if self.ended or self.complete:
            raise ProtocolError("frame after END")
        frame = memoryview(metadata).cast("B")
        if len(frame) > self.limits.max_frame_bytes:
            raise ProtocolError("frame exceeds metadata byte limit")
        if len(frame) < HEADER.size:
            raise ProtocolError("truncated frame header")
        magic, kind, sequence, offset = HEADER.unpack_from(frame)
        if magic != MAGIC or kind not in (DATA, END):
            raise ProtocolError("unsupported frame magic or kind")
        if sequence != self.stats.frames:
            raise ProtocolError("out-of-order or duplicate frame")
        if offset != self.stats.ipc_bytes:
            raise ProtocolError("noncontiguous byte offset")
        payload = frame[HEADER.size:]
        if kind == END:
            if payload:
                raise ProtocolError("END must not contain a payload")
            self.ended = True
        else:
            if not payload:
                raise ProtocolError("DATA must contain a payload")
            if offset + len(payload) > self.limits.max_total_bytes:
                raise ProtocolError(
                    "encoded IPC stream exceeds total byte limit")
            self.file.write(payload)
        self.stats.add(len(frame), len(payload))

    def finish(self):
        """Call only after the Flight stream has reached a clean EOF."""
        if not self.ended:
            raise ProtocolError("truncated stream: missing END")
        if self.stats.ipc_bytes < len(IPC_EOS):
            raise ProtocolError("truncated IPC stream")
        self.file.seek(-len(IPC_EOS), io.SEEK_END)
        if self.file.read() != IPC_EOS:
            raise ProtocolError("missing IPC end-of-stream marker")
        self.complete = True

    def read_table(self):
        if not self.complete:
            raise ProtocolError("stream has not reached a clean EOF")
        self.file.seek(0)
        with pa.ipc.open_stream(self.file) as reader:
            # Compression is deliberately not part of this example protocol.
            # A total encoded-byte cap is not a decoded-allocation guarantee.
            table = reader.read_all()
        if self.file.tell() != self.stats.ipc_bytes:
            raise ProtocolError("trailing bytes after IPC stream")
        return table

    def replay(self, send_metadata):
        """Echo opaque IPC bytes. The client performs semantic IPC decoding."""
        if not self.complete:
            raise ProtocolError("stream has not reached a clean EOF")
        self.file.seek(0)
        with FrameSink(send_metadata, self.limits) as sink:
            while chunk := self.file.read(
                    self.limits.max_frame_bytes - HEADER.size):
                sink.write(chunk)
            sink.finish()
            return sink.stats


def _parse_limits(value):
    if not isinstance(value, dict) or set(value) != {
            "max_frame_bytes", "max_total_bytes"}:
        raise ProtocolError("invalid limits object")
    return Limits(**value)


def exchange_descriptor(limits):
    command = PROTOCOL.encode() + b"\n" + json.dumps(asdict(limits)).encode()
    return flight.FlightDescriptor.for_command(command)


def negotiate(client, requested, options=None):
    """Require opt-in before upload; never fall back automatically."""
    try:
        results = iter(client.do_action(
            flight.Action(CAPABILITIES_ACTION, b""), options=options))
        first = next(results, None)
        if first is None or len(first.body) > 1024:
            raise ProtocolError("invalid capability response")
        document = json.loads(first.body.to_pybytes())
        if next(results, None) is not None:
            raise ProtocolError("expected one capability response")
    except pa.ArrowNotImplementedError as exc:
        raise ProtocolError("server does not support " + PROTOCOL) from exc
    if not isinstance(document, dict) or document.get("protocol") != PROTOCOL:
        raise ProtocolError("server does not support " + PROTOCOL)
    offered = _parse_limits(document.get("limits"))
    return Limits(min(requested.max_frame_bytes, offered.max_frame_bytes),
                  min(requested.max_total_bytes, offered.max_total_bytes))


def _receive(reader, receiver):
    for chunk in reader:
        if chunk.data is not None or chunk.app_metadata is None:
            raise ProtocolError("expected metadata-only FlightData")
        receiver.accept(chunk.app_metadata)
    receiver.finish()


class ChunkedEchoServer(flight.FlightServerBase):
    """Disk-backed echo endpoint for trusted localhost experiments."""

    def __init__(self, location=None, limits=None, temp_dir=None):
        self.limits = limits or Limits()
        self.temp_dir = temp_dir
        super().__init__(location)

    def list_actions(self, context):
        return [(CAPABILITIES_ACTION, "Experimental chunked IPC limits")]

    def do_action(self, context, action):
        if action.type != CAPABILITIES_ACTION:
            raise pa.ArrowNotImplementedError("unsupported action")
        document = {"protocol": PROTOCOL, "limits": asdict(self.limits)}
        yield flight.Result(json.dumps(document).encode())

    def do_exchange(self, context, descriptor, reader, writer):
        prefix = PROTOCOL.encode() + b"\n"
        if (descriptor.descriptor_type != flight.DescriptorType.CMD or
                len(descriptor.command) > 512 or
                not descriptor.command.startswith(prefix)):
            raise pa.ArrowNotImplementedError("unsupported exchange command")
        limits = _parse_limits(json.loads(descriptor.command[len(prefix):]))
        if (limits.max_frame_bytes > self.limits.max_frame_bytes or
                limits.max_total_bytes > self.limits.max_total_bytes):
            raise ProtocolError("requested limits exceed server capabilities")

        def send(metadata):
            if context.is_cancelled():
                raise flight.FlightCancelledError("exchange cancelled")
            writer.write_metadata(metadata)

        with Reassembler(limits, self.temp_dir) as receiver:
            for chunk in reader:
                if context.is_cancelled():
                    raise flight.FlightCancelledError("exchange cancelled")
                if chunk.data is not None or chunk.app_metadata is None:
                    raise ProtocolError("expected metadata-only FlightData")
                receiver.accept(chunk.app_metadata)
            receiver.finish()
            receiver.replay(send)


@dataclass
class ExchangeResult:
    table: pa.Table
    sent: Stats
    received: Stats
    limits: Limits


def exchange(client, schema, batches, limits=None, options=None,
             temp_dir=None):
    """Negotiate, send lazy batches, half-close, then receive the echoed table.

    The peer must drain the complete upload before replying, as this helper is
    deliberately sequential. Both client and server temporary files close on
    success, malformed input, source failure, timeout, and cancellation.
    """
    limits = negotiate(client, limits or Limits(), options)
    writer, reader = client.do_exchange(exchange_descriptor(limits),
                                        options=options)
    try:
        sent = send_batches(writer.write_metadata, schema, batches, limits)
        writer.done_writing()
        with Reassembler(limits, temp_dir) as receiver:
            _receive(reader, receiver)
            table = receiver.read_table()
            received = receiver.stats
        writer.close()
        return ExchangeResult(table, sent, received, limits)
    except BaseException:
        reader.cancel()
        with contextlib.suppress(Exception):
            writer.close()
        raise
