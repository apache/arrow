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

import base64
import contextlib
import os
import socket
import tempfile
import threading
import time

import pytest

import pyarrow as pa

from pyarrow.compat import tobytes


flight = pytest.importorskip("pyarrow.flight")


def simple_ints_table():
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    return pa.Table.from_arrays(data, names=['some_ints'])


def simple_dicts_table():
    dict_values = pa.array(["foo", "baz", "quux"], type=pa.utf8())
    data = [
        pa.chunked_array([
            pa.DictionaryArray.from_arrays([1, 0, None], dict_values),
            pa.DictionaryArray.from_arrays([2, 1], dict_values)]),
    ]
    return pa.Table.from_arrays(data, names=['some_dicts'])


class ConstantFlightServer(flight.FlightServerBase):
    """A Flight server that always returns the same data.

    See ARROW-4796: this server implementation will segfault if Flight
    does not properly hold a reference to the Table object.
    """

    def __init__(self):
        super(ConstantFlightServer, self).__init__()
        # Ticket -> Table
        self.table_factories = {
            b'ints': simple_ints_table,
            b'dicts': simple_dicts_table,
        }

    def do_get(self, context, ticket):
        # Return a fresh table, so that Flight is the only one keeping a
        # reference.
        table = self.table_factories[ticket.ticket]()
        return flight.RecordBatchStream(table)


class EchoFlightServer(flight.FlightServerBase):
    """A Flight server that returns the last data uploaded."""

    def __init__(self):
        super(EchoFlightServer, self).__init__()
        self.last_message = None

    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self.last_message)

    def do_put(self, context, descriptor, reader):
        self.last_message = reader.read_all()


class EchoStreamFlightServer(EchoFlightServer):
    """An echo server that streams individual record batches."""

    def do_get(self, context, ticket):
        return flight.GeneratorStream(
            self.last_message.schema,
            self.last_message.to_batches(chunksize=1024))

    def list_actions(self, context):
        return []

    def do_action(self, context, action):
        if action.type == "who-am-i":
            return iter([flight.Result(context.peer_identity())])
        raise NotImplementedError


class GetInfoFlightServer(flight.FlightServerBase):
    """A Flight server that tests GetFlightInfo."""

    def get_flight_info(self, context, descriptor):
        return flight.FlightInfo(
            pa.schema([('a', pa.int32())]),
            descriptor,
            [
                flight.FlightEndpoint(b'', ['grpc://test']),
                flight.FlightEndpoint(
                    b'',
                    [flight.Location.for_grpc_tcp('localhost', 5005)],
                ),
            ],
            -1,
            -1,
        )


class CheckTicketFlightServer(flight.FlightServerBase):
    """A Flight server that compares the given ticket to an expected value."""

    def __init__(self, expected_ticket):
        super(CheckTicketFlightServer, self).__init__()
        self.expected_ticket = expected_ticket

    def do_get(self, context, ticket):
        assert self.expected_ticket == ticket.ticket
        data1 = [pa.array([-10, -5, 0, 5, 10], type=pa.int32())]
        table = pa.Table.from_arrays(data1, names=['a'])
        return flight.RecordBatchStream(table)

    def do_put(self, context, descriptor, reader):
        self.last_message = reader.read_all()


class InvalidStreamFlightServer(flight.FlightServerBase):
    """A Flight server that tries to return messages with differing schemas."""

    schema = pa.schema([('a', pa.int32())])

    def do_get(self, context, ticket):
        data1 = [pa.array([-10, -5, 0, 5, 10], type=pa.int32())]
        data2 = [pa.array([-10.0, -5.0, 0.0, 5.0, 10.0], type=pa.float64())]
        assert data1.type != data2.type
        table1 = pa.Table.from_arrays(data1, names=['a'])
        table2 = pa.Table.from_arrays(data2, names=['a'])
        assert table1.schema == self.schema

        return flight.GeneratorStream(self.schema, [table1, table2])


class SlowFlightServer(flight.FlightServerBase):
    """A Flight server that delays its responses to test timeouts."""

    def do_action(self, context, action):
        time.sleep(0.5)
        return iter([])


class HttpBasicServerAuthHandler(flight.ServerAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, creds):
        super().__init__()
        self.creds = creds

    def authenticate(self, outgoing, incoming):
        pass

    def is_valid(self, token):
        if not token:
            raise ValueError("unauthenticated: token not provided")
        token = base64.b64decode(token)
        username, password = token.split(b':')
        if username not in self.creds:
            raise ValueError("unknown user")
        if self.creds[username] != password:
            raise ValueError("wrong password")
        return username


class HttpBasicClientAuthHandler(flight.ClientAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, username, password):
        super().__init__()
        self.username = tobytes(username)
        self.password = tobytes(password)

    def authenticate(self, outgoing, incoming):
        pass

    def get_token(self):
        return base64.b64encode(self.username + b':' + self.password)


class TokenServerAuthHandler(flight.ServerAuthHandler):
    """An example implementation of authentication via handshake."""

    def __init__(self, creds):
        super().__init__()
        self.creds = creds

    def authenticate(self, outgoing, incoming):
        username = incoming.read()
        password = incoming.read()
        if username in self.creds and self.creds[username] == password:
            outgoing.write(base64.b64encode(b'secret:' + username))
        else:
            raise ValueError("unauthenticated: invalid username/password")

    def is_valid(self, token):
        token = base64.b64decode(token)
        if not token.startswith(b'secret:'):
            raise ValueError("unauthenticated: invalid token")
        return token[7:]


class TokenClientAuthHandler(flight.ClientAuthHandler):
    """An example implementation of authentication via handshake."""

    def __init__(self, username, password):
        super().__init__()
        self.username = username
        self.password = password
        self.token = b''

    def authenticate(self, outgoing, incoming):
        outgoing.write(self.username)
        outgoing.write(self.password)
        self.token = incoming.read()

    def get_token(self):
        return self.token


@contextlib.contextmanager
def flight_server(server_base, *args, **kwargs):
    """Spawn a Flight server on a free port, shutting it down when done."""
    auth_handler = kwargs.pop('auth_handler', None)
    location = kwargs.pop('location', None)

    if location is None:
        # Find a free port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with contextlib.closing(sock) as sock:
            sock.bind(('', 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = sock.getsockname()[1]
        location = flight.Location.for_grpc_tcp("localhost", port)
    else:
        port = None

    ctor_kwargs = kwargs
    server_instance = server_base(*args, **ctor_kwargs)

    def _server_thread():
        server_instance.run(location, auth_handler=auth_handler)

    thread = threading.Thread(target=_server_thread, daemon=True)
    thread.start()

    yield location

    server_instance.shutdown()
    thread.join()


def test_flight_do_get_ints():
    """Try a simple do_get call."""
    table = simple_ints_table()

    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        data = client.do_get(flight.Ticket(b'ints')).read_all()
        assert data.equals(table)


def test_flight_do_get_dicts():
    table = simple_dicts_table()

    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        data = client.do_get(flight.Ticket(b'dicts')).read_all()
        assert data.equals(table)


def test_flight_do_get_ticket():
    """Make sure Tickets get passed to the server."""
    data1 = [pa.array([-10, -5, 0, 5, 10], type=pa.int32())]
    table = pa.Table.from_arrays(data1, names=['a'])
    with flight_server(
            CheckTicketFlightServer,
            expected_ticket=b'the-ticket',
    ) as server_location:
        client = flight.FlightClient.connect(server_location)
        data = client.do_get(flight.Ticket(b'the-ticket')).read_all()
        assert data.equals(table)


def test_flight_get_info():
    """Make sure FlightEndpoint accepts string and object URIs."""
    with flight_server(GetInfoFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        info = client.get_flight_info(flight.FlightDescriptor.for_command(b''))
        assert info.total_records == -1
        assert info.total_bytes == -1
        assert info.schema == pa.schema([('a', pa.int32())])
        assert len(info.endpoints) == 2
        assert len(info.endpoints[0].locations) == 1
        assert info.endpoints[0].locations[0] == flight.Location('grpc://test')
        assert info.endpoints[1].locations[0] == \
            flight.Location.for_grpc_tcp('localhost', 5005)


@pytest.mark.skipif(os.name == 'nt',
                    reason="Unix sockets can't be tested on Windows")
def test_flight_domain_socket():
    """Try a simple do_get call over a Unix domain socket."""
    table = simple_ints_table()

    with tempfile.NamedTemporaryFile() as sock:
        sock.close()
        location = flight.Location.for_grpc_unix(sock.name)
        with flight_server(ConstantFlightServer,
                           location=location) as server_location:
            client = flight.FlightClient.connect(server_location)
            data = client.do_get(flight.Ticket(b'ints')).read_all()
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

    with flight_server(EchoFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        writer = client.do_put(flight.FlightDescriptor.for_path('test'),
                               data.schema)
        # Write a single giant chunk
        writer.write_table(data, 10 * 1024 * 1024)
        writer.close()
        result = client.do_get(flight.Ticket(b'')).read_all()
        assert result.equals(data)


def test_flight_generator_stream():
    """Try downloading a flight of RecordBatches in a GeneratorStream."""
    data = pa.Table.from_arrays([
        pa.array(range(0, 10 * 1024))
    ], names=['a'])

    with flight_server(EchoStreamFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        writer = client.do_put(flight.FlightDescriptor.for_path('test'),
                               data.schema)
        writer.write_table(data)
        writer.close()
        result = client.do_get(flight.Ticket(b'')).read_all()
        assert result.equals(data)


def test_flight_invalid_generator_stream():
    """Try streaming data with mismatched schemas."""
    with flight_server(InvalidStreamFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        with pytest.raises(pa.ArrowException):
            client.do_get(flight.Ticket(b'')).read_all()


def test_timeout_fires():
    """Make sure timeouts fire on slow requests."""
    # Do this in a separate thread so that if it fails, we don't hang
    # the entire test process
    with flight_server(SlowFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("", b"")
        options = flight.FlightCallOptions(timeout=0.2)
        with pytest.raises(pa.ArrowIOError, match="Deadline Exceeded"):
            list(client.do_action(action, options=options))


def test_timeout_passes():
    """Make sure timeouts do not fire on fast requests."""
    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        options = flight.FlightCallOptions(timeout=5.0)
        client.do_get(flight.Ticket(b'ints'), options=options).read_all()


basic_auth_handler = HttpBasicServerAuthHandler(creds={
    b"test": b"p4ssw0rd",
})

token_auth_handler = TokenServerAuthHandler(creds={
    b"test": b"p4ssw0rd",
})


def test_http_basic_unauth():
    """Test that auth fails when not authenticated."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("who-am-i", b"")
        with pytest.raises(pa.ArrowException, match=".*unauthenticated.*"):
            list(client.do_action(action))


def test_http_basic_auth():
    """Test a Python implementation of HTTP basic authentication."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("who-am-i", b"")
        client.authenticate(HttpBasicClientAuthHandler('test', 'p4ssw0rd'))
        identity = next(client.do_action(action))
        assert identity.body.to_pybytes() == b'test'


def test_http_basic_auth_invalid_password():
    """Test that auth fails with the wrong password."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("who-am-i", b"")
        client.authenticate(HttpBasicClientAuthHandler('test', 'wrong'))
        with pytest.raises(pa.ArrowException, match=".*wrong password.*"):
            next(client.do_action(action))


def test_token_auth():
    """Test an auth mechanism that uses a handshake."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=token_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("who-am-i", b"")
        client.authenticate(TokenClientAuthHandler('test', 'p4ssw0rd'))
        identity = next(client.do_action(action))
        assert identity.body.to_pybytes() == b'test'


def test_token_auth_invalid():
    """Test an auth mechanism that uses a handshake."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=token_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        with pytest.raises(pa.ArrowException, match=".*unauthenticated.*"):
            client.authenticate(TokenClientAuthHandler('test', 'wrong'))


def test_location_invalid():
    """Test constructing invalid URIs."""
    with pytest.raises(pa.ArrowInvalid, match=".*Cannot parse URI:.*"):
        flight.FlightClient.connect("%")

    server = ConstantFlightServer()
    with pytest.raises(pa.ArrowInvalid, match=".*Cannot parse URI:.*"):
        server.run("%")
