# -*- coding: utf-8 -*-
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

import contextlib
import socket
import threading

import pytest

import pyarrow as pa


flight = pytest.importorskip("pyarrow.flight")


class ConstantFlightServer(flight.FlightServerBase):
    """A Flight server that always returns the same data.

    See ARROW-4796: this server implementation will segfault if Flight
    does not properly hold a reference to the Table object.
    """

    def do_get(self, ticket):
        data = [
            pa.array([-10, -5, 0, 5, 10])
        ]
        table = pa.Table.from_arrays(data, names=['a'])
        return flight.RecordBatchStream(table)


class EchoFlightServer(flight.FlightServerBase):
    """A Flight server that returns the last data uploaded."""

    def __init__(self):
        super(EchoFlightServer, self).__init__()
        self.last_message = None

    def do_get(self, ticket):
        return flight.RecordBatchStream(self.last_message)

    def do_put(self, descriptor, reader):
        self.last_message = reader.read_all()


@contextlib.contextmanager
def flight_server(server_base, *args, **kwargs):
    """Spawn a Flight server on a free port, shutting it down when done."""
    # Find a free port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with contextlib.closing(sock) as sock:
        sock.bind(('', 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = sock.getsockname()[1]

    server_instance = server_base(*args, **kwargs)

    def _server_thread():
        server_instance.run(port)

    thread = threading.Thread(target=_server_thread, daemon=True)
    thread.start()

    yield port

    server_instance.shutdown()
    thread.join()


def test_flight_do_get():
    """Try a simple do_get call."""
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    with flight_server(ConstantFlightServer) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        data = client.do_get(flight.Ticket(b''), table.schema).read_all()
        assert data.equals(table)


@pytest.mark.slow
def test_flight_large_message():
    """Try sending/receiving a large message via Flight.

    See ARROW-4421: by default, gRPC won't allow us to send messages >
    4MiB in size.
    """
    data = pa.Table.from_arrays([
        pa.array(range(0, 10 * 1024 * 1024))
    ], names=['a'])

    with flight_server(EchoFlightServer) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        writer = client.do_put(flight.FlightDescriptor.for_path('test'),
                               data.schema)
        # Write a single giant chunk
        writer.write_table(data, 10 * 1024 * 1024)
        writer.close()
        result = client.do_get(flight.Ticket(b''), data.schema).read_all()
        assert result.equals(data)
