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

# cython: language_level = 3

import collections
import enum

from cython.operator cimport dereference as deref

from pyarrow.compat import frombytes, tobytes
from pyarrow.lib cimport *
from pyarrow.lib import as_buffer
from pyarrow.includes.libarrow_flight cimport *
from pyarrow.ipc import _ReadPandasOption
import pyarrow.lib as lib


cdef CFlightCallOptions DEFAULT_CALL_OPTIONS


cdef class FlightCallOptions:
    """RPC-layer options for a Flight call."""

    cdef:
        CFlightCallOptions options

    def __init__(self, timeout=None):
        if timeout is not None:
            self.options.timeout = CTimeoutDuration(timeout)

    @staticmethod
    cdef CFlightCallOptions* unwrap(obj):
        if not obj:
            return &DEFAULT_CALL_OPTIONS
        elif isinstance(obj, FlightCallOptions):
            return &((<FlightCallOptions> obj).options)
        raise TypeError("Expected a FlightCallOptions object, not "
                        "'{}'".format(type(obj)))


cdef class Action:
    """An action executable on a Flight service."""
    cdef:
        CAction action

    def __init__(self, action_type, buf):
        self.action.type = tobytes(action_type)
        self.action.body = pyarrow_unwrap_buffer(as_buffer(buf))

    @property
    def type(self):
        return frombytes(self.action.type)

    def body(self):
        return pyarrow_wrap_buffer(self.action.body)


_ActionType = collections.namedtuple('_ActionType', ['type', 'description'])


class ActionType(_ActionType):
    """A type of action executable on a Flight service."""

    def make_action(self, buf):
        """Create an Action with this type."""
        return Action(self.type, buf)


cdef class Result:
    """A result from executing an Action."""
    cdef:
        unique_ptr[CResult] result

    def __init__(self, buf):
        self.result.reset(new CResult())
        self.result.get().body = pyarrow_unwrap_buffer(as_buffer(buf))

    @property
    def body(self):
        """Get the Buffer containing the result."""
        return pyarrow_wrap_buffer(self.result.get().body)


class DescriptorType(enum.Enum):
    UNKNOWN = 0
    PATH = 1
    CMD = 2


cdef class FlightDescriptor:
    """A description of a data stream available from a Flight service."""
    cdef:
        CFlightDescriptor descriptor

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use "
                        "`pyarrow.flight.FlightDescriptor.for_{path,command}` "
                        "function instead."
                        .format(self.__class__.__name__))

    @staticmethod
    def for_path(*path):
        """Create a FlightDescriptor for a resource path."""
        cdef FlightDescriptor result = \
            FlightDescriptor.__new__(FlightDescriptor)
        result.descriptor.type = CDescriptorTypePath
        result.descriptor.path = [tobytes(p) for p in path]
        return result

    @staticmethod
    def for_command(command):
        """Create a FlightDescriptor for an opaque command."""
        cdef FlightDescriptor result = \
            FlightDescriptor.__new__(FlightDescriptor)
        result.descriptor.type = CDescriptorTypeCmd
        result.descriptor.cmd = tobytes(command)
        return result

    @property
    def descriptor_type(self):
        if self.descriptor.type == CDescriptorTypeUnknown:
            return DescriptorType.UNKNOWN
        elif self.descriptor.type == CDescriptorTypePath:
            return DescriptorType.PATH
        elif self.descriptor.type == CDescriptorTypeCmd:
            return DescriptorType.CMD
        raise RuntimeError("Invalid descriptor type!")

    @property
    def command(self):
        """Get the command for this descriptor."""
        if self.descriptor_type != DescriptorType.CMD:
            return None
        return self.descriptor.cmd

    @property
    def path(self):
        """Get the path for this descriptor."""
        if self.descriptor_type != DescriptorType.PATH:
            return None
        return self.descriptor.path

    def __repr__(self):
        return "<FlightDescriptor type: {!r}>".format(self.descriptor_type)


class Ticket:
    """A ticket for requesting a Flight stream."""
    def __init__(self, ticket):
        self.ticket = ticket

    def __repr__(self):
        return '<Ticket {}>'.format(self.ticket)


class Location(collections.namedtuple('Location', ['host', 'port'])):
    """A location where a Flight stream is available."""


cdef class FlightEndpoint:
    """A Flight stream, along with the ticket and locations to access it."""
    cdef:
        CFlightEndpoint endpoint

    def __init__(self, ticket, locations):
        """Create a FlightEndpoint from a ticket and list of locations.

        Parameters
        ----------
        ticket : Ticket or bytes
            the ticket needed to access this flight
        locations : list of Location or tuples of (host, port)
            locations where this flight is available
        """
        cdef:
            CLocation c_location = CLocation()

        if isinstance(ticket, Ticket):
            self.endpoint.ticket.ticket = tobytes(ticket.ticket)
        else:
            self.endpoint.ticket.ticket = tobytes(ticket)

        for location in locations:
            # Accepts Location namedtuple or tuple
            c_location.host = tobytes(location[0])
            c_location.port = location[1]
            self.endpoint.locations.push_back(c_location)

    @property
    def ticket(self):
        return Ticket(self.endpoint.ticket.ticket)

    @property
    def locations(self):
        return [Location(frombytes(location.host), location.port)
                for location in self.endpoint.locations]


cdef class FlightInfo:
    """A description of a Flight stream."""
    cdef:
        unique_ptr[CFlightInfo] info

    def __init__(self, Schema schema, FlightDescriptor descriptor, endpoints,
                 total_records, total_bytes):
        """Create a FlightInfo object from a schema, descriptor, and endpoints.

        Parameters
        ----------
        schema : Schema
            the schema of the data in this flight.
        descriptor : FlightDescriptor
            the descriptor for this flight.
        endpoints : list of FlightEndpoint
            a list of endpoints where this flight is available.
        total_records : int
            the total records in this flight, or -1 if unknown
        total_bytes : int
            the total bytes in this flight, or -1 if unknown
        """
        cdef:
            shared_ptr[CSchema] c_schema = pyarrow_unwrap_schema(schema)
            vector[CFlightEndpoint] c_endpoints

        for endpoint in endpoints:
            if isinstance(endpoint, FlightEndpoint):
                c_endpoints.push_back((<FlightEndpoint> endpoint).endpoint)
            else:
                raise TypeError('Endpoint {} is not instance of'
                                ' FlightEndpoint'.format(endpoint))

        check_status(CreateFlightInfo(c_schema,
                                      descriptor.descriptor,
                                      c_endpoints,
                                      total_records,
                                      total_bytes, &self.info))

    @property
    def total_records(self):
        """The total record count of this flight, or -1 if unknown."""
        return self.info.get().total_records()

    @property
    def total_bytes(self):
        """The size in bytes of the data in this flight, or -1 if unknown."""
        return self.info.get().total_bytes()

    @property
    def schema(self):
        """The schema of the data in this flight."""
        cdef:
            shared_ptr[CSchema] schema
            CDictionaryMemo dummy_memo

        check_status(self.info.get().GetSchema(&dummy_memo, &schema))
        return pyarrow_wrap_schema(schema)

    @property
    def descriptor(self):
        """The descriptor of the data in this flight."""
        cdef FlightDescriptor result = \
            FlightDescriptor.__new__(FlightDescriptor)
        result.descriptor = self.info.get().descriptor()
        return result

    @property
    def endpoints(self):
        """The endpoints where this flight is available."""
        # TODO: get Cython to iterate over reference directly
        cdef:
            vector[CFlightEndpoint] endpoints = self.info.get().endpoints()
            FlightEndpoint py_endpoint

        result = []
        for endpoint in endpoints:
            py_endpoint = FlightEndpoint.__new__(FlightEndpoint)
            py_endpoint.endpoint = endpoint
            result.append(py_endpoint)
        return result


cdef class FlightRecordBatchReader(_CRecordBatchReader, _ReadPandasOption):
    cdef dict __dict__


cdef class FlightRecordBatchWriter(_CRecordBatchWriter):
    pass


cdef class FlightClient:
    """A client to a Flight service."""
    cdef:
        unique_ptr[CFlightClient] client

    def __init__(self):
        raise TypeError("Do not call {}'s constructor directly, use "
                        "`pyarrow.flight.FlightClient.connect` instead."
                        .format(self.__class__.__name__))

    @staticmethod
    def connect(*args):
        """Connect to a Flight service on the given host and port."""
        cdef:
            FlightClient result = FlightClient.__new__(FlightClient)
            int c_port = 0
            c_string c_host

        if len(args) == 1:
            # Accept namedtuple or plain tuple
            c_host = tobytes(args[0][0])
            c_port = args[0][1]
        elif len(args) == 2:
            # Accept separate host, port
            c_host = tobytes(args[0])
            c_port = args[1]
        else:
            raise TypeError("FlightClient.connect() takes 1 "
                            "or 2 arguments ({} given)".format(len(args)))

        with nogil:
            check_status(CFlightClient.Connect(c_host, c_port, &result.client))

        return result

    def authenticate(self, auth_handler, options: FlightCallOptions = None):
        """Authenticate to the server."""
        cdef:
            unique_ptr[CClientAuthHandler] handler
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)

        if not isinstance(auth_handler, ClientAuthHandler):
            raise TypeError(
                "FlightClient.authenticate takes a ClientAuthHandler, "
                "not '{}'".format(type(auth_handler)))
        handler.reset((<ClientAuthHandler> auth_handler).to_handler())
        with nogil:
            check_status(self.client.get().Authenticate(deref(c_options),
                                                        move(handler)))

    def list_actions(self, options: FlightCallOptions = None):
        """List the actions available on a service."""
        cdef:
            vector[CActionType] results
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)

        with nogil:
            check_status(
                self.client.get().ListActions(deref(c_options), &results))

        result = []
        for action_type in results:
            py_action = ActionType(frombytes(action_type.type),
                                   frombytes(action_type.description))
            result.append(py_action)

        return result

    def do_action(self, action: Action, options: FlightCallOptions = None):
        """Execute an action on a service."""
        cdef:
            unique_ptr[CResultStream] results
            Result result
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)
        with nogil:
            check_status(self.client.get().DoAction(deref(c_options),
                                                    action.action, &results))

        while True:
            result = Result.__new__(Result)
            with nogil:
                check_status(results.get().Next(&result.result))
                if result.result == NULL:
                    break
            yield result

    def list_flights(self, options: FlightCallOptions = None):
        """List the flights available on a service."""
        cdef:
            unique_ptr[CFlightListing] listing
            FlightInfo result
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)
            CCriteria c_criteria

        with nogil:
            check_status(self.client.get().ListFlights(deref(c_options),
                                                       c_criteria, &listing))

        while True:
            result = FlightInfo.__new__(FlightInfo)
            with nogil:
                check_status(listing.get().Next(&result.info))
                if result.info == NULL:
                    break
            yield result

    def get_flight_info(self, descriptor: FlightDescriptor,
                        options: FlightCallOptions = None):
        """Request information about an available flight."""
        cdef:
            FlightInfo result = FlightInfo.__new__(FlightInfo)
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)

        with nogil:
            check_status(self.client.get().GetFlightInfo(
                deref(c_options), descriptor.descriptor, &result.info))

        return result

    def do_get(self, ticket: Ticket, options: FlightCallOptions = None):
        """Request the data for a flight."""
        cdef:
            # TODO: introduce unwrap
            CTicket c_ticket
            unique_ptr[CRecordBatchReader] reader
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)

        c_ticket.ticket = ticket.ticket
        with nogil:
            check_status(
                self.client.get().DoGet(deref(c_options), c_ticket, &reader))
        result = FlightRecordBatchReader()
        result.reader.reset(reader.release())
        return result

    def do_put(self, descriptor: FlightDescriptor, schema: Schema,
               options: FlightCallOptions = None):
        """Upload data to a flight."""
        cdef:
            shared_ptr[CSchema] c_schema = pyarrow_unwrap_schema(schema)
            unique_ptr[CRecordBatchWriter] writer
            CFlightCallOptions* c_options = FlightCallOptions.unwrap(options)

        with nogil:
            check_status(self.client.get().DoPut(
                deref(c_options), descriptor.descriptor, c_schema, &writer))
        result = FlightRecordBatchWriter()
        result.writer.reset(writer.release())
        return result


cdef class FlightDataStream:
    """Abstract base class for Flight data streams."""

    cdef CFlightDataStream* to_stream(self):
        """Create the C++ data stream for the backing Python object.

        We don't expose the C++ object to Python, so we can manage its
        lifetime from the Cython/C++ side.
        """
        raise NotImplementedError


cdef class RecordBatchStream(FlightDataStream):
    """A Flight data stream backed by RecordBatches."""
    cdef:
        object data_source

    def __init__(self, data_source):
        """Create a RecordBatchStream from a data source.

        Parameters
        ----------
        data_source : RecordBatchReader or Table
        """
        if (not isinstance(data_source, _CRecordBatchReader) and
                not isinstance(data_source, lib.Table)):
            raise TypeError("Expected RecordBatchReader or Table, "
                            "but got: {}".format(type(data_source)))
        self.data_source = data_source

    cdef CFlightDataStream* to_stream(self):
        cdef:
            shared_ptr[CRecordBatchReader] reader
        if isinstance(self.data_source, _CRecordBatchReader):
            reader = (<_CRecordBatchReader> self.data_source).reader
        elif isinstance(self.data_source, lib.Table):
            table = (<Table> self.data_source).table
            reader.reset(new TableBatchReader(deref(table)))
        else:
            raise RuntimeError("Can't construct RecordBatchStream "
                               "from type {}".format(type(self.data_source)))
        return new CRecordBatchStream(reader)


cdef class GeneratorStream(FlightDataStream):
    """A Flight data stream backed by a Python generator."""
    cdef:
        shared_ptr[CSchema] schema
        object generator
        # A substream currently being consumed by the client, if
        # present. Produced by the generator.
        unique_ptr[CFlightDataStream] current_stream

    def __init__(self, schema, generator):
        """Create a GeneratorStream from a Python generator.

        Parameters
        ----------
        schema : Schema
            The schema for the data to be returned.

        generator : iterator or iterable
            The generator should yield other FlightDataStream objects,
            Tables, RecordBatches, or RecordBatchReaders.
        """
        self.schema = pyarrow_unwrap_schema(schema)
        self.generator = iter(generator)

    cdef CFlightDataStream* to_stream(self):
        cdef:
            function[cb_data_stream_next] callback = &_data_stream_next
        return new CPyGeneratorFlightDataStream(self, self.schema, callback)


cdef class ServerCallContext:
    """Per-call state/context."""
    cdef:
        const CServerCallContext* context

    def peer_identity(self):
        """Get the identity of the authenticated peer.

        May be the empty string.
        """
        return tobytes(self.context.peer_identity())

    @staticmethod
    cdef ServerCallContext wrap(const CServerCallContext& context):
        cdef ServerCallContext result = \
            ServerCallContext.__new__(ServerCallContext)
        result.context = &context
        return result


cdef class ServerAuthReader:
    """A reader for messages from the client during an auth handshake."""
    cdef:
        CServerAuthReader* reader

    def read(self):
        cdef c_string token
        if not self.reader:
            raise ValueError("Cannot use ServerAuthReader outside "
                             "ServerAuthHandler.authenticate")
        with nogil:
            check_status(self.reader.Read(&token))
        return token

    cdef void poison(self):
        """Prevent further usage of this object.

        This object is constructed by taking a pointer to a reference,
        so we want to make sure Python users do not access this after
        the reference goes away.
        """
        self.reader = NULL

    @staticmethod
    cdef ServerAuthReader wrap(CServerAuthReader* reader):
        cdef ServerAuthReader result = \
            ServerAuthReader.__new__(ServerAuthReader)
        result.reader = reader
        return result


cdef class ServerAuthSender:
    """A writer for messages to the client during an auth handshake."""
    cdef:
        CServerAuthSender* sender

    def write(self, message):
        cdef c_string c_message = tobytes(message)
        if not self.sender:
            raise ValueError("Cannot use ServerAuthSender outside "
                             "ServerAuthHandler.authenticate")
        with nogil:
            check_status(self.sender.Write(c_message))

    cdef void poison(self):
        """Prevent further usage of this object.

        This object is constructed by taking a pointer to a reference,
        so we want to make sure Python users do not access this after
        the reference goes away.
        """
        self.sender = NULL

    @staticmethod
    cdef ServerAuthSender wrap(CServerAuthSender* sender):
        cdef ServerAuthSender result = \
            ServerAuthSender.__new__(ServerAuthSender)
        result.sender = sender
        return result


cdef class ClientAuthReader:
    """A reader for messages from the server during an auth handshake."""
    cdef:
        CClientAuthReader* reader

    def read(self):
        cdef c_string token
        if not self.reader:
            raise ValueError("Cannot use ClientAuthReader outside "
                             "ClientAuthHandler.authenticate")
        with nogil:
            check_status(self.reader.Read(&token))
        return token

    cdef void poison(self):
        """Prevent further usage of this object.

        This object is constructed by taking a pointer to a reference,
        so we want to make sure Python users do not access this after
        the reference goes away.
        """
        self.reader = NULL

    @staticmethod
    cdef ClientAuthReader wrap(CClientAuthReader* reader):
        cdef ClientAuthReader result = \
            ClientAuthReader.__new__(ClientAuthReader)
        result.reader = reader
        return result


cdef class ClientAuthSender:
    """A writer for messages to the server during an auth handshake."""
    cdef:
        CClientAuthSender* sender

    def write(self, message):
        cdef c_string c_message = tobytes(message)
        if not self.sender:
            raise ValueError("Cannot use ClientAuthSender outside "
                             "ClientAuthHandler.authenticate")
        with nogil:
            check_status(self.sender.Write(c_message))

    cdef void poison(self):
        """Prevent further usage of this object.

        This object is constructed by taking a pointer to a reference,
        so we want to make sure Python users do not access this after
        the reference goes away.
        """
        self.sender = NULL

    @staticmethod
    cdef ClientAuthSender wrap(CClientAuthSender* sender):
        cdef ClientAuthSender result = \
            ClientAuthSender.__new__(ClientAuthSender)
        result.sender = sender
        return result


cdef void _data_stream_next(void* self, CFlightPayload* payload) except *:
    """Callback for implementing FlightDataStream in Python."""
    cdef:
        unique_ptr[CFlightDataStream] data_stream

    py_stream = <object> self
    if not isinstance(py_stream, GeneratorStream):
        raise RuntimeError("self object in callback is not GeneratorStream")
    stream = <GeneratorStream> py_stream

    if stream.current_stream != nullptr:
        check_status(stream.current_stream.get().Next(payload))
        # If the stream ended, see if there's another stream from the
        # generator
        if payload.ipc_message.metadata != nullptr:
            return
        stream.current_stream.reset(nullptr)

    try:
        result = next(stream.generator)
    except StopIteration:
        payload.ipc_message.metadata.reset(<CBuffer*> nullptr)
        return

    if isinstance(result, (Table, _CRecordBatchReader)):
        result = RecordBatchStream(result)

    stream_schema = pyarrow_wrap_schema(stream.schema)
    if isinstance(result, FlightDataStream):
        data_stream = unique_ptr[CFlightDataStream](
            (<FlightDataStream> result).to_stream())
        substream_schema = pyarrow_wrap_schema(data_stream.get().schema())
        if substream_schema != stream_schema:
            raise ValueError("Got a FlightDataStream whose schema does not "
                             "match the declared schema of this "
                             "GeneratorStream. "
                             "Got: {}\nExpected: {}".format(substream_schema,
                                                            stream_schema))
        stream.current_stream.reset(
            new CPyFlightDataStream(result, move(data_stream)))
        _data_stream_next(self, payload)
    elif isinstance(result, RecordBatch):
        batch = <RecordBatch> result
        if batch.schema != stream_schema:
            raise ValueError("Got a RecordBatch whose schema does not "
                             "match the declared schema of this "
                             "GeneratorStream. "
                             "Got: {}\nExpected: {}".format(batch.schema,
                                                            stream_schema))
        check_status(_GetRecordBatchPayload(
            deref(batch.batch),
            c_default_memory_pool(),
            &payload.ipc_message))
    else:
        raise TypeError("GeneratorStream must be initialized with "
                        "an iterator of FlightDataStream, Table, "
                        "RecordBatch, or RecordBatchStreamReader objects, "
                        "not {}.".format(type(result)))


cdef void _list_flights(void* self, const CServerCallContext& context,
                        const CCriteria* c_criteria,
                        unique_ptr[CFlightListing]* listing) except *:
    """Callback for implementing ListFlights in Python."""
    cdef:
        vector[CFlightInfo] flights
    result = (<object> self).list_flights(ServerCallContext.wrap(context),
                                          c_criteria.expression)
    for info in result:
        if not isinstance(info, FlightInfo):
            raise TypeError("FlightServerBase.list_flights must return "
                            "FlightInfo instances, but got {}".format(
                                type(info)))
        flights.push_back(deref((<FlightInfo> info).info.get()))
    listing.reset(new CSimpleFlightListing(flights))


cdef void _get_flight_info(void* self, const CServerCallContext& context,
                           CFlightDescriptor c_descriptor,
                           unique_ptr[CFlightInfo]* info) except *:
    """Callback for implementing Flight servers in Python."""
    cdef:
        FlightDescriptor py_descriptor = \
            FlightDescriptor.__new__(FlightDescriptor)
    py_descriptor.descriptor = c_descriptor
    result = (<object> self).get_flight_info(ServerCallContext.wrap(context),
                                             py_descriptor)
    if not isinstance(result, FlightInfo):
        raise TypeError("FlightServerBase.get_flight_info must return "
                        "a FlightInfo instance, but got {}".format(
                            type(result)))
    info.reset(new CFlightInfo(deref((<FlightInfo> result).info.get())))


cdef void _do_put(void* self, const CServerCallContext& context,
                  unique_ptr[CFlightMessageReader] reader) except *:
    """Callback for implementing Flight servers in Python."""
    cdef:
        FlightRecordBatchReader py_reader = FlightRecordBatchReader()
        FlightDescriptor descriptor = \
            FlightDescriptor.__new__(FlightDescriptor)

    descriptor.descriptor = reader.get().descriptor()
    py_reader.reader.reset(reader.release())
    (<object> self).do_put(ServerCallContext.wrap(context), descriptor,
                           py_reader)


cdef void _do_get(void* self, const CServerCallContext& context,
                  CTicket ticket,
                  unique_ptr[CFlightDataStream]* stream) except *:
    """Callback for implementing Flight servers in Python."""
    cdef:
        unique_ptr[CFlightDataStream] data_stream

    py_ticket = Ticket(ticket.ticket)
    result = (<object> self).do_get(ServerCallContext.wrap(context),
                                    py_ticket)
    if not isinstance(result, FlightDataStream):
        raise TypeError("FlightServerBase.do_get must return "
                        "a FlightDataStream")
    data_stream = unique_ptr[CFlightDataStream](
        (<FlightDataStream> result).to_stream())
    stream[0] = unique_ptr[CFlightDataStream](
        new CPyFlightDataStream(result, move(data_stream)))


cdef void _do_action_result_next(void* self,
                                 unique_ptr[CResult]* result) except *:
    """Callback for implementing Flight servers in Python."""
    try:
        action_result = next(<object> self)
        if not isinstance(action_result, Result):
            raise TypeError("Result of FlightServerBase.do_action must "
                            "return an iterator of Result objects")
        result.reset(new CResult(deref((<Result> action_result).result.get())))
    except StopIteration:
        result.reset(nullptr)


cdef void _do_action(void* self, const CServerCallContext& context,
                     const CAction& action,
                     unique_ptr[CResultStream]* result) except *:
    """Callback for implementing Flight servers in Python."""
    cdef:
        function[cb_result_next] ptr = &_do_action_result_next
    py_action = Action(action.type, pyarrow_wrap_buffer(action.body))
    responses = (<object> self).do_action(ServerCallContext.wrap(context),
                                          py_action)
    result.reset(new CPyFlightResultStream(responses, ptr))


cdef void _list_actions(void* self, const CServerCallContext& context,
                        vector[CActionType]* actions) except *:
    """Callback for implementing Flight servers in Python."""
    cdef:
        CActionType action_type
    # Method should return a list of ActionTypes or similar tuple
    result = (<object> self).list_actions()
    for action in result:
        action_type.type = tobytes(action[0])
        action_type.description = tobytes(action[1])
        actions.push_back(action_type)


cdef void _server_authenticate(void* self, CServerAuthSender* outgoing,
                               CServerAuthReader* incoming) except *:
    """Callback for implementing authentication in Python."""
    sender = ServerAuthSender.wrap(outgoing)
    reader = ServerAuthReader.wrap(incoming)
    try:
        (<object> self).authenticate(sender, reader)
    finally:
        sender.poison()
        reader.poison()


cdef void _is_valid(void* self, const c_string& token,
                    c_string* peer_identity) except *:
    """Callback for implementing authentication in Python."""
    cdef c_string c_result
    c_result = tobytes((<object> self).is_valid(token))
    peer_identity[0] = c_result


cdef void _client_authenticate(void* self, CClientAuthSender* outgoing,
                               CClientAuthReader* incoming) except *:
    """Callback for implementing authentication in Python."""
    sender = ClientAuthSender.wrap(outgoing)
    reader = ClientAuthReader.wrap(incoming)
    try:
        (<object> self).authenticate(sender, reader)
    finally:
        sender.poison()
        reader.poison()


cdef void _get_token(void* self, c_string* token) except *:
    """Callback for implementing authentication in Python."""
    cdef c_string c_result
    c_result = tobytes((<object> self).get_token())
    token[0] = c_result


cdef class ServerAuthHandler:
    """Authentication middleware for a server."""

    def authenticate(self, outgoing, incoming):
        """Conduct the handshake with the client."""
        raise NotImplementedError

    def is_valid(self, token):
        """Validate a client token, returning their identity.

        May return an empty string (if the auth mechanism does not
        name the peer) or raise an exception (if the token is
        invalid).

        """
        raise NotImplementedError

    cdef PyServerAuthHandler* to_handler(self):
        cdef PyServerAuthHandlerVtable vtable
        vtable.authenticate = _server_authenticate
        vtable.is_valid = _is_valid
        return new PyServerAuthHandler(self, vtable)


cdef class ClientAuthHandler:
    """Authentication plugin for a client."""

    def authenticate(self, outgoing, incoming):
        """Conduct the handshake with the server."""
        raise NotImplementedError

    def get_token(self):
        """Get the auth token for a call."""
        raise NotImplementedError

    cdef PyClientAuthHandler* to_handler(self):
        cdef PyClientAuthHandlerVtable vtable
        vtable.authenticate = _client_authenticate
        vtable.get_token = _get_token
        return new PyClientAuthHandler(self, vtable)


cdef class FlightServerBase:
    """A Flight service definition."""

    cdef:
        unique_ptr[PyFlightServer] server

    def run(self, port, auth_handler=None):
        cdef:
            PyFlightServerVtable vtable = PyFlightServerVtable()
            int c_port = port
            PyFlightServer* c_server
            unique_ptr[CServerAuthHandler] c_auth_handler

        if auth_handler:
            if not isinstance(auth_handler, ServerAuthHandler):
                raise TypeError("auth_handler must be a ServerAuthHandler, "
                                "not a '{}'".format(type(auth_handler)))
            c_auth_handler.reset(
                (<ServerAuthHandler> auth_handler).to_handler())

        vtable.list_flights = &_list_flights
        vtable.get_flight_info = &_get_flight_info
        vtable.do_put = &_do_put
        vtable.do_get = &_do_get
        vtable.list_actions = &_list_actions
        vtable.do_action = &_do_action

        c_server = new PyFlightServer(self, vtable)
        self.server.reset(c_server)
        with nogil:
            check_status(c_server.Init(move(c_auth_handler), c_port))
            check_status(c_server.ServeWithSignals())

    def list_flights(self, context, criteria):
        raise NotImplementedError

    def get_flight_info(self, context, descriptor):
        raise NotImplementedError

    def do_put(self, context, descriptor, reader):
        raise NotImplementedError

    def do_get(self, context, ticket):
        raise NotImplementedError

    def list_actions(self, context):
        raise NotImplementedError

    def do_action(self, context, action):
        raise NotImplementedError

    def shutdown(self):
        """Shut down the server, blocking until current requests finish.

        Do not call this directly from the implementation of a Flight
        method, as then the server will block forever waiting for that
        request to finish. Instead, call this method from a background
        thread.
        """
        # Must not hold the GIL: shutdown waits for pending RPCs to
        # complete. Holding the GIL means Python-implemented Flight
        # methods will never get to run, so this will hang
        # indefinitely.
        with nogil:
            if self.server.get() != NULL:
                self.server.get().Shutdown()
