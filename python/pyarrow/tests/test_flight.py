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
import struct
import tempfile
import threading
import time
import traceback

import pytest
import pyarrow as pa

from pyarrow.compat import tobytes
from pyarrow.util import pathlib

try:
    from pyarrow import flight
    from pyarrow.flight import (
        FlightServerBase, ServerAuthHandler, ClientAuthHandler
    )
except ImportError:
    flight = None
    FlightServerBase = object
    ServerAuthHandler, ClientAuthHandler = object, object


# Marks all of the tests in this module
# Ignore these with pytest ... -m 'not flight'
pytestmark = pytest.mark.flight


def test_import():
    # So we see the ImportError somewhere
    import pyarrow.flight  # noqa


def resource_root():
    """Get the path to the test resources directory."""
    if not os.environ.get("ARROW_TEST_DATA"):
        raise RuntimeError("Test resources not found; set "
                           "ARROW_TEST_DATA to <repo root>/testing")
    return pathlib.Path(os.environ["ARROW_TEST_DATA"]) / "flight"


def read_flight_resource(path):
    """Get the contents of a test resource file."""
    root = resource_root()
    if not root:
        return None
    try:
        with (root / path).open("rb") as f:
            return f.read()
    except FileNotFoundError:
        raise RuntimeError(
            "Test resource {} not found; did you initialize the "
            "test resource submodule?\n{}".format(root / path,
                                                  traceback.format_exc()))


def example_tls_certs():
    """Get the paths to test TLS certificates."""
    return {
        "root_cert": read_flight_resource("root-ca.pem"),
        "certificates": [
            flight.CertKeyPair(
                cert=read_flight_resource("cert0.pem"),
                key=read_flight_resource("cert0.key"),
            ),
            flight.CertKeyPair(
                cert=read_flight_resource("cert1.pem"),
                key=read_flight_resource("cert1.key"),
            ),
        ]
    }


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


class ConstantFlightServer(FlightServerBase):
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


class MetadataFlightServer(FlightServerBase):
    """A Flight server that numbers incoming/outgoing data."""

    def do_get(self, context, ticket):
        data = [
            pa.array([-10, -5, 0, 5, 10])
        ]
        table = pa.Table.from_arrays(data, names=['a'])
        return flight.GeneratorStream(
            table.schema,
            self.number_batches(table))

    def do_put(self, context, descriptor, reader, writer):
        counter = 0
        expected_data = [-10, -5, 0, 5, 10]
        while True:
            try:
                batch, buf = reader.read_chunk()
                assert batch.equals(pa.RecordBatch.from_arrays(
                    [pa.array([expected_data[counter]])],
                    ['a']
                ))
                assert buf is not None
                client_counter, = struct.unpack('<i', buf.to_pybytes())
                assert counter == client_counter
                writer.write(struct.pack('<i', counter))
                counter += 1
            except StopIteration:
                return

    @staticmethod
    def number_batches(table):
        for idx, batch in enumerate(table.to_batches()):
            buf = struct.pack('<i', idx)
            yield batch, buf


class EchoFlightServer(FlightServerBase):
    """A Flight server that returns the last data uploaded."""

    def __init__(self, expected_schema=None):
        super(EchoFlightServer, self).__init__()
        self.last_message = None
        self.expected_schema = expected_schema

    def do_get(self, context, ticket):
        return flight.RecordBatchStream(self.last_message)

    def do_put(self, context, descriptor, reader, writer):
        if self.expected_schema:
            assert self.expected_schema == reader.schema
        self.last_message = reader.read_all()


class EchoStreamFlightServer(EchoFlightServer):
    """An echo server that streams individual record batches."""

    def do_get(self, context, ticket):
        return flight.GeneratorStream(
            self.last_message.schema,
            self.last_message.to_batches(max_chunksize=1024))

    def list_actions(self, context):
        return []

    def do_action(self, context, action):
        if action.type == "who-am-i":
            return iter([flight.Result(context.peer_identity())])
        raise NotImplementedError


class GetInfoFlightServer(FlightServerBase):
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

    def get_schema(self, context, descriptor):
        info = self.get_flight_info(context, descriptor)
        return flight.SchemaResult(info.schema)


class ListActionsFlightServer(FlightServerBase):
    """A Flight server that tests ListActions."""

    @classmethod
    def expected_actions(cls):
        return [
            ("action-1", "description"),
            ("action-2", ""),
            flight.ActionType("action-3", "more detail"),
        ]

    def list_actions(self, context):
        for action in self.expected_actions():
            yield action


class ListActionsErrorFlightServer(FlightServerBase):
    """A Flight server that tests ListActions."""

    def list_actions(self, context):
        yield ("action-1", "")
        yield "foo"


class CheckTicketFlightServer(FlightServerBase):
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


class InvalidStreamFlightServer(FlightServerBase):
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


class SlowFlightServer(FlightServerBase):
    """A Flight server that delays its responses to test timeouts."""

    def do_get(self, context, ticket):
        return flight.GeneratorStream(pa.schema([('a', pa.int32())]),
                                      self.slow_stream())

    def do_action(self, context, action):
        time.sleep(0.5)
        return iter([])

    @staticmethod
    def slow_stream():
        data1 = [pa.array([-10, -5, 0, 5, 10], type=pa.int32())]
        yield pa.Table.from_arrays(data1, names=['a'])
        # The second message should never get sent; the client should
        # cancel before we send this
        time.sleep(10)
        yield pa.Table.from_arrays(data1, names=['a'])


class ErrorFlightServer(FlightServerBase):
    """A Flight server that uses all the Flight-specific errors."""

    def do_action(self, context, action):
        if action.type == "internal":
            raise flight.FlightInternalError("foo")
        elif action.type == "timedout":
            raise flight.FlightTimedOutError("foo")
        elif action.type == "cancel":
            raise flight.FlightCancelledError("foo")
        elif action.type == "unauthenticated":
            raise flight.FlightUnauthenticatedError("foo")
        elif action.type == "unauthorized":
            raise flight.FlightUnauthorizedError("foo")
        raise NotImplementedError

    def list_flights(self, context, criteria):
        yield flight.FlightInfo(
            pa.schema([]),
            flight.FlightDescriptor.for_path('/foo'),
            [],
            -1, -1
        )
        raise flight.FlightInternalError("foo")


class HttpBasicServerAuthHandler(ServerAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, creds):
        super(HttpBasicServerAuthHandler, self).__init__()
        self.creds = creds

    def authenticate(self, outgoing, incoming):
        buf = incoming.read()
        auth = flight.BasicAuth.deserialize(buf)
        if auth.username not in self.creds:
            raise flight.FlightUnauthenticatedError("unknown user")
        if self.creds[auth.username] != auth.password:
            raise flight.FlightUnauthenticatedError("wrong password")
        outgoing.write(tobytes(auth.username))

    def is_valid(self, token):
        if not token:
            raise flight.FlightUnauthenticatedError("token not provided")
        if token not in self.creds:
            raise flight.FlightUnauthenticatedError("unknown user")
        return token


class HttpBasicClientAuthHandler(ClientAuthHandler):
    """An example implementation of HTTP basic authentication."""

    def __init__(self, username, password):
        super(HttpBasicClientAuthHandler, self).__init__()
        self.basic_auth = flight.BasicAuth(username, password)
        self.token = None

    def authenticate(self, outgoing, incoming):
        auth = self.basic_auth.serialize()
        outgoing.write(auth)
        self.token = incoming.read()

    def get_token(self):
        return self.token


class TokenServerAuthHandler(ServerAuthHandler):
    """An example implementation of authentication via handshake."""

    def __init__(self, creds):
        super(TokenServerAuthHandler, self).__init__()
        self.creds = creds

    def authenticate(self, outgoing, incoming):
        username = incoming.read()
        password = incoming.read()
        if username in self.creds and self.creds[username] == password:
            outgoing.write(base64.b64encode(b'secret:' + username))
        else:
            raise flight.FlightUnauthenticatedError(
                "invalid username/password")

    def is_valid(self, token):
        token = base64.b64decode(token)
        if not token.startswith(b'secret:'):
            raise flight.FlightUnauthenticatedError("invalid token")
        return token[7:]


class TokenClientAuthHandler(ClientAuthHandler):
    """An example implementation of authentication via handshake."""

    def __init__(self, username, password):
        super(TokenClientAuthHandler, self).__init__()
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
    tls_certificates = kwargs.pop('tls_certificates', None)
    location = kwargs.pop('location', None)
    try_connect = kwargs.pop('try_connect', True)
    connect_args = kwargs.pop('connect_args', {})

    if location is None:
        # Find a free port
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        with contextlib.closing(sock) as sock:
            sock.bind(('', 0))
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            port = sock.getsockname()[1]
        ctor = flight.Location.for_grpc_tcp
        if tls_certificates:
            ctor = flight.Location.for_grpc_tls
        location = ctor("localhost", port)
    else:
        port = None

    ctor_kwargs = kwargs
    server_instance = server_base(*args, **ctor_kwargs)
    # The server instance needs to be initialized before shutdown()
    # can be called
    server_instance.init(location,
                         auth_handler=auth_handler,
                         tls_certificates=tls_certificates)

    def _server_thread():
        server_instance.run()

    thread = threading.Thread(target=_server_thread, daemon=True)
    thread.start()

    # Wait for server to start
    if try_connect:
        deadline = time.time() + 5.0
        client = flight.FlightClient.connect(location, **connect_args)
        while True:
            try:
                list(client.list_flights())
            except Exception as e:
                if 'Connect Failed' in str(e):
                    if time.time() < deadline:
                        time.sleep(0.025)
                        continue
                    else:
                        raise
            break

    try:
        yield location
    finally:
        server_instance.shutdown()
        thread.join(3.0)


def test_flight_do_get_ints():
    """Try a simple do_get call."""
    table = simple_ints_table()

    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        data = client.do_get(flight.Ticket(b'ints')).read_all()
        assert data.equals(table)


@pytest.mark.pandas
def test_do_get_ints_pandas():
    """Try a simple do_get call."""
    table = simple_ints_table()

    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        data = client.do_get(flight.Ticket(b'ints')).read_pandas()
        assert list(data['some_ints']) == table.column(0).to_pylist()


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


def test_flight_get_schema():
    """Make sure GetSchema returns correct schema."""
    with flight_server(GetInfoFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        info = client.get_schema(flight.FlightDescriptor.for_command(b''))
        assert info.schema == pa.schema([('a', pa.int32())])


def test_list_actions():
    """Make sure the return type of ListActions is validated."""
    # ARROW-6392
    with flight_server(ListActionsErrorFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        with pytest.raises(pa.ArrowException, match=".*unknown error.*"):
            list(client.list_actions())

    with flight_server(ListActionsFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        assert list(client.list_actions()) == \
            ListActionsFlightServer.expected_actions()


@pytest.mark.skipif(os.name == 'nt',
                    reason="Unix sockets can't be tested on Windows")
def test_flight_domain_socket():
    """Try a simple do_get call over a Unix domain socket."""
    with tempfile.NamedTemporaryFile() as sock:
        sock.close()
        location = flight.Location.for_grpc_unix(sock.name)
        with flight_server(ConstantFlightServer,
                           location=location) as server_location:
            client = flight.FlightClient.connect(server_location)

            reader = client.do_get(flight.Ticket(b'ints'))
            table = simple_ints_table()
            assert reader.schema.equals(table.schema)
            data = reader.read_all()
            assert data.equals(table)

            reader = client.do_get(flight.Ticket(b'dicts'))
            table = simple_dicts_table()
            assert reader.schema.equals(table.schema)
            data = reader.read_all()
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

    with flight_server(EchoFlightServer,
                       expected_schema=data.schema) as server_location:
        client = flight.FlightClient.connect(server_location)
        writer, _ = client.do_put(flight.FlightDescriptor.for_path('test'),
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
        writer, _ = client.do_put(flight.FlightDescriptor.for_path('test'),
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
        # gRPC error messages change based on version, so don't look
        # for a particular error
        with pytest.raises(flight.FlightTimedOutError):
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


@pytest.mark.slow
def test_http_basic_unauth():
    """Test that auth fails when not authenticated."""
    with flight_server(EchoStreamFlightServer,
                       auth_handler=basic_auth_handler) as server_location:
        client = flight.FlightClient.connect(server_location)
        action = flight.Action("who-am-i", b"")
        with pytest.raises(flight.FlightUnauthenticatedError,
                           match=".*unauthenticated.*"):
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
        with pytest.raises(flight.FlightUnauthenticatedError,
                           match=".*wrong password.*"):
            client.authenticate(HttpBasicClientAuthHandler('test', 'wrong'))
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
        with pytest.raises(flight.FlightUnauthenticatedError):
            client.authenticate(TokenClientAuthHandler('test', 'wrong'))


def test_location_invalid():
    """Test constructing invalid URIs."""
    with pytest.raises(pa.ArrowInvalid, match=".*Cannot parse URI:.*"):
        flight.FlightClient.connect("%")

    server = ConstantFlightServer()
    with pytest.raises(pa.ArrowInvalid, match=".*Cannot parse URI:.*"):
        server.init("%")


def test_location_unknown_scheme():
    """Test creating locations for unknown schemes."""
    assert flight.Location("s3://foo").uri == b"s3://foo"
    assert flight.Location("https://example.com/bar.parquet").uri == \
        b"https://example.com/bar.parquet"


@pytest.mark.slow
@pytest.mark.requires_testing_data
def test_tls_fails():
    """Make sure clients cannot connect when cert verification fails."""
    certs = example_tls_certs()

    with flight_server(
            ConstantFlightServer, tls_certificates=certs["certificates"],
            connect_args=dict(tls_root_certs=certs["root_cert"]),
    ) as server_location:
        # Ensure client doesn't connect when certificate verification
        # fails (this is a slow test since gRPC does retry a few times)
        client = flight.FlightClient.connect(server_location)
        # gRPC error messages change based on version, so don't look
        # for a particular error
        with pytest.raises(flight.FlightUnavailableError):
            client.do_get(flight.Ticket(b'ints'))


@pytest.mark.requires_testing_data
def test_tls_do_get():
    """Try a simple do_get call over TLS."""
    table = simple_ints_table()
    certs = example_tls_certs()

    with flight_server(
            ConstantFlightServer, tls_certificates=certs["certificates"],
            connect_args=dict(tls_root_certs=certs["root_cert"]),
    ) as server_location:
        client = flight.FlightClient.connect(
            server_location, tls_root_certs=certs["root_cert"])
        data = client.do_get(flight.Ticket(b'ints')).read_all()
        assert data.equals(table)


@pytest.mark.requires_testing_data
def test_tls_override_hostname():
    """Check that incorrectly overriding the hostname fails."""
    certs = example_tls_certs()

    with flight_server(
            ConstantFlightServer, tls_certificates=certs["certificates"],
            connect_args=dict(tls_root_certs=certs["root_cert"]),
    ) as server_location:
        client = flight.FlightClient.connect(
            server_location, tls_root_certs=certs["root_cert"],
            override_hostname="fakehostname")
        with pytest.raises(flight.FlightUnavailableError):
            client.do_get(flight.Ticket(b'ints'))


def test_flight_do_get_metadata():
    """Try a simple do_get call with metadata."""
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    batches = []
    with flight_server(MetadataFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        reader = client.do_get(flight.Ticket(b''))
        idx = 0
        while True:
            try:
                batch, metadata = reader.read_chunk()
                batches.append(batch)
                server_idx, = struct.unpack('<i', metadata.to_pybytes())
                assert idx == server_idx
                idx += 1
            except StopIteration:
                break
        data = pa.Table.from_batches(batches)
        assert data.equals(table)


def test_flight_do_put_metadata():
    """Try a simple do_put call with metadata."""
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    with flight_server(MetadataFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        writer, metadata_reader = client.do_put(
            flight.FlightDescriptor.for_path(''),
            table.schema)
        with writer:
            for idx, batch in enumerate(table.to_batches(max_chunksize=1)):
                metadata = struct.pack('<i', idx)
                writer.write_with_metadata(batch, metadata)
                buf = metadata_reader.read()
                assert buf is not None
                server_idx, = struct.unpack('<i', buf.to_pybytes())
                assert idx == server_idx


@pytest.mark.slow
def test_cancel_do_get():
    """Test canceling a DoGet operation on the client side."""
    with flight_server(ConstantFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        reader = client.do_get(flight.Ticket(b'ints'))
        reader.cancel()
        with pytest.raises(flight.FlightCancelledError, match=".*Cancel.*"):
            reader.read_chunk()


@pytest.mark.slow
def test_cancel_do_get_threaded():
    """Test canceling a DoGet operation from another thread."""
    with flight_server(SlowFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        reader = client.do_get(flight.Ticket(b'ints'))

        read_first_message = threading.Event()
        stream_canceled = threading.Event()
        result_lock = threading.Lock()
        raised_proper_exception = threading.Event()

        def block_read():
            reader.read_chunk()
            read_first_message.set()
            stream_canceled.wait(timeout=5)
            try:
                reader.read_chunk()
            except flight.FlightCancelledError:
                with result_lock:
                    raised_proper_exception.set()

        thread = threading.Thread(target=block_read, daemon=True)
        thread.start()
        read_first_message.wait(timeout=5)
        reader.cancel()
        stream_canceled.set()
        thread.join(timeout=1)

        with result_lock:
            assert raised_proper_exception.is_set()


def test_roundtrip_types():
    """Make sure serializable types round-trip."""
    ticket = flight.Ticket("foo")
    assert ticket == flight.Ticket.deserialize(ticket.serialize())

    desc = flight.FlightDescriptor.for_command("test")
    assert desc == flight.FlightDescriptor.deserialize(desc.serialize())

    desc = flight.FlightDescriptor.for_path("a", "b", "test.arrow")
    assert desc == flight.FlightDescriptor.deserialize(desc.serialize())

    info = flight.FlightInfo(
        pa.schema([('a', pa.int32())]),
        desc,
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
    info2 = flight.FlightInfo.deserialize(info.serialize())
    assert info.schema == info2.schema
    assert info.descriptor == info2.descriptor
    assert info.total_bytes == info2.total_bytes
    assert info.total_records == info2.total_records
    assert info.endpoints == info2.endpoints


def test_roundtrip_errors():
    """Ensure that Flight errors propagate from server to client."""
    with flight_server(ErrorFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        with pytest.raises(flight.FlightInternalError, match=".*foo.*"):
            list(client.do_action(flight.Action("internal", b"")))
        with pytest.raises(flight.FlightTimedOutError, match=".*foo.*"):
            list(client.do_action(flight.Action("timedout", b"")))
        with pytest.raises(flight.FlightCancelledError, match=".*foo.*"):
            list(client.do_action(flight.Action("cancel", b"")))
        with pytest.raises(flight.FlightUnauthenticatedError, match=".*foo.*"):
            list(client.do_action(flight.Action("unauthenticated", b"")))
        with pytest.raises(flight.FlightUnauthorizedError, match=".*foo.*"):
            list(client.do_action(flight.Action("unauthorized", b"")))
        with pytest.raises(flight.FlightInternalError, match=".*foo.*"):
            list(client.list_flights())


def test_do_put_independent_read_write():
    """Ensure that separate threads can read/write on a DoPut."""
    # ARROW-6063: previously this would cause gRPC to abort when the
    # writer was closed (due to simultaneous reads), or would hang
    # forever.
    data = [
        pa.array([-10, -5, 0, 5, 10])
    ]
    table = pa.Table.from_arrays(data, names=['a'])

    with flight_server(MetadataFlightServer) as server_location:
        client = flight.FlightClient.connect(server_location)
        writer, metadata_reader = client.do_put(
            flight.FlightDescriptor.for_path(''),
            table.schema)

        count = [0]

        def _reader_thread():
            while metadata_reader.read() is not None:
                count[0] += 1

        thread = threading.Thread(target=_reader_thread)
        thread.start()

        batches = table.to_batches(max_chunksize=1)
        with writer:
            for idx, batch in enumerate(batches):
                metadata = struct.pack('<i', idx)
                writer.write_with_metadata(batch, metadata)
            # Causes the server to stop writing and end the call
            writer.done_writing()
            # Thus reader thread will break out of loop
            thread.join()
        # writer.close() won't segfault since reader thread has
        # stopped
        assert count[0] == len(batches)
