.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at

..   http://www.apache.org/licenses/LICENSE-2.0

.. Unless required by applicable law or agreed to in writing,
.. software distributed under the License is distributed on an
.. "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
.. KIND, either express or implied.  See the License for the
.. specific language governing permissions and limitations
.. under the License.

.. default-domain:: cpp
.. highlight:: cpp

================
Arrow Flight RPC
================

Arrow Flight is an RPC framework for efficient transfer of Flight data
over the network. See :doc:`../format/Flight` for full details on
the protocol, or :doc:`./api/flight` for API docs.

Writing a Flight Service
========================

Servers are subclasses of :class:`arrow::flight::FlightServerBase`. To
implement individual RPCs, override the RPC methods on this class.

.. code-block:: cpp

   class MyFlightServer : public FlightServerBase {
     Status ListFlights(const ServerCallContext& context, const Criteria* criteria,
                        std::unique_ptr<FlightListing>* listings) override {
       std::vector<FlightInfo> flights = ...;
       *listings = std::unique_ptr<FlightListing>(new SimpleFlightListing(flights));
       return Status::OK();
     }
   };

Each RPC method always takes a
:class:`arrow::flight::ServerCallContext` for common parameters and
returns a :class:`arrow::Status` to indicate success or
failure. Flight-specific error codes can be returned via
:func:`arrow::flight::MakeFlightError`.

RPC methods that return a value in addition to a status will use an
out parameter, as shown above. Often, there are helper classes
providing basic implementations of these out parameters. For instance,
above, :class:`arrow::flight::SimpleFlightListing` uses a vector of
:class:`arrow::flight::FlightInfo` objects as the result of a
``ListFlights`` RPC.

To start a server, create a :class:`arrow::flight::Location` to
specify where to listen, and call
:func:`arrow::flight::FlightServerBase::Init`. This will start the
server, but won't block the rest of the program. Use
:func:`arrow::flight::FlightServerBase::SetShutdownOnSignals` to
enable stopping the server if an interrupt signal is received, then
call :func:`arrow::flight::FlightServerBase::Serve` to block until the
server stops.

.. code-block:: cpp

   std::unique_ptr<arrow::flight::FlightServerBase> server;
   // Initialize server
   arrow::flight::Location location;
   // Listen to all interfaces on a free port
   ARROW_CHECK_OK(arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0, &location));
   arrow::flight::FlightServerOptions options(location);

   // Start the server
   ARROW_CHECK_OK(server->Init(options));
   // Exit with a clean error code (0) on SIGTERM
   ARROW_CHECK_OK(server->SetShutdownOnSignals({SIGTERM}));

   std::cout << "Server listening on localhost:" << server->port() << std::endl;
   ARROW_CHECK_OK(server->Serve());


Enabling TLS and Authentication
-------------------------------

TLS can be enabled by providing a certificate and key pair to
:func:`FlightServerBase::Init
<arrow::flight::FlightServerBase::Init>`. Additionally, use
:func:`Location::ForGrpcTls <arrow::flight::Location::ForGrpcTls>` to
construct the :class:`arrow::flight::Location` to listen on.

Similarly, authentication can be enabled by providing an
implementation of :class:`ServerAuthHandler
<arrow::flight::ServerAuthHandler>`. Authentication consists of two
parts: on initial client connection, the server and client
authentication implementations can perform any negotiation needed;
then, on each RPC thereafter, the client provides a token. The server
authentication handler validates the token and provides the identity
of the client. This identity can be obtained from the
:class:`arrow::flight::ServerCallContext`.

Using the Flight Client
=======================

To connect to a Flight service, create an instance of
:class:`arrow::flight::FlightClient` by calling :func:`Connect
<arrow::flight::FlightClient::Connect>`. This takes a Location and
returns the client through an out parameter. To authenticate, call
:func:`Authenticate <arrow::flight::FlightClient::Authenticate>` with
the desired client authentication implementation.

Each RPC method returns :class:`arrow::Status` to indicate the
success/failure of the request. Any other return values are specified
through out parameters. They also take an optional :class:`options
<arrow::flight::FlightCallOptions>` parameter that allows specifying a
timeout for the call.

Alternative Transports
======================

The standard transport for Arrow Flight is gRPC_. The C++
implementation also experimentally supports a transport based on
UCX_. To use it, use the protocol scheme ``ucx:`` when starting a
server or creating a client.

UCX Transport
-------------

Not all features of the gRPC transport are supported. See
:ref:`status-flight-rpc` for details. Also note these specific
caveats:

- The server creates an independent UCP worker for each client. This
  consumes more resources but provides better throughput.
- The client creates an independent UCP worker for each RPC
  call. Again, this trades off resource consumption for
  performance. This also means that unlike with gRPC, it is
  essentially equivalent to make all calls with a single client or
  with multiple clients.
- The UCX transport attempts to avoid copies where possible. In some
  cases, it can directly reuse UCX-allocated buffers to back
  :class:`arrow::Buffer` objects, however, this will also extend the
  lifetime of associated UCX resources beyond the lifetime of the
  Flight client or server object.
- Depending on the transport that UCX itself selects, you may find
  that increasing ``UCX_MM_SEG_SIZE`` from the default (around 8KB) to
  around 60KB improves performance (UCX will copy more data in a
  single call).

.. _gRPC: https://grpc.io/
.. _UCX: https://openucx.org/
