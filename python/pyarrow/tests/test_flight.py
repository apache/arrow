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
import socket
import threading

import pytest

import pyarrow as pa

from pyarrow.compat import tobytes


flight = pytest.importorskip("pyarrow.flight")


class ConstantFlightServer(flight.FlightServerBase):
    """A Flight server that always returns the same data.

    See ARROW-4796: this server implementation will segfault if Flight
    does not properly hold a reference to the Table object.
    """

    def do_get(self, context, ticket):
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
    # Find a free port
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    with contextlib.closing(sock) as sock:
        sock.bind(('', 0))
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        port = sock.getsockname()[1]

    auth_handler = kwargs.get('auth_handler')
    ctor_kwargs = kwargs
    if auth_handler:
        del ctor_kwargs['auth_handler']
    server_instance = server_base(*args, **ctor_kwargs)

    def _server_thread():
        server_instance.run(port, auth_handler=auth_handler)

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
        data = client.do_get(flight.Ticket(b'')).read_all()
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
        result = client.do_get(flight.Ticket(b'')).read_all()
        assert result.equals(data)


def test_flight_generator_stream():
    """Try downloading a flight of RecordBatches in a GeneratorStream."""
    data = pa.Table.from_arrays([
        pa.array(range(0, 10 * 1024))
    ], names=['a'])

    with flight_server(EchoStreamFlightServer) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        writer = client.do_put(flight.FlightDescriptor.for_path('test'),
                               data.schema)
        writer.write_table(data)
        writer.close()
        result = client.do_get(flight.Ticket(b'')).read_all()
        assert result.equals(data)


def test_flight_invalid_generator_stream():
    """Try streaming data with mismatched schemas."""
    with flight_server(InvalidStreamFlightServer) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        with pytest.raises(pa.ArrowException):
            client.do_get(flight.Ticket(b'')).read_all()


basic_auth_handler = HttpBasicServerAuthHandler(creds={
    b"test": b"p4ssw0rd",
})

token_auth_handler = TokenServerAuthHandler(creds={
    b"test": b"p4ssw0rd",
})


def test_http_basic_unauth():
    """Test that auth fails when not authenticated."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        action = flight.Action("who-am-i", b"")
        with pytest.raises(pa.ArrowException, match=".*unauthenticated.*"):
            list(client.do_action(action))


def test_http_basic_auth():
    """Test a Python implementation of HTTP basic authentication."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        action = flight.Action("who-am-i", b"")
        client.authenticate(HttpBasicClientAuthHandler('test', 'p4ssw0rd'))
        identity = next(client.do_action(action))
        assert identity.body.to_pybytes() == b'test'


def test_http_basic_auth_invalid_password():
    """Test that auth fails with the wrong password."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        action = flight.Action("who-am-i", b"")
        client.authenticate(HttpBasicClientAuthHandler('test', 'wrong'))
        with pytest.raises(pa.ArrowException, match=".*wrong password.*"):
            next(client.do_action(action))


def test_token_auth():
    """Test an auth mechanism that uses a handshake."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=token_auth_handler) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        action = flight.Action("who-am-i", b"")
        client.authenticate(TokenClientAuthHandler('test', 'p4ssw0rd'))
        identity = next(client.do_action(action))
        assert identity.body.to_pybytes() == b'test'


def test_token_auth_invalid():
    """Test an auth mechanism that uses a handshake."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=token_auth_handler) as server_port:
        client = flight.FlightClient.connect('localhost', server_port)
        with pytest.raises(pa.ArrowException, match=".*unauthenticated.*"):
            client.authenticate(TokenClientAuthHandler('test', 'wrong'))
