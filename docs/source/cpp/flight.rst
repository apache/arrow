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
over the network.

.. seealso::

   :doc:`Flight protocol documentation <../format/Flight>`
        Documentation of the Flight protocol, including how to use
        Flight conceptually.

   :doc:`Flight API documentation <./api/flight>`
        C++ API documentation listing all of the various client and
        server types.

   `C++ Cookbook <https://arrow.apache.org/cookbook/cpp/flight.html>`_
        Recipes for using Arrow Flight in C++.

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

Using the Flight Client
=======================

To connect to a Flight service, create an instance of
:class:`arrow::flight::FlightClient` by calling :func:`Connect
<arrow::flight::FlightClient::Connect>`.

Each RPC method returns :class:`arrow::Result` to indicate the
success/failure of the request, and the result object if the request
succeeded. Some calls are streaming calls, so they will return a
reader and/or a writer object; the final call status isn't known until
the stream is completed.

Cancellation and Timeouts
=========================

When making a call, clients can optionally provide
:class:`FlightCallOptions <arrow::flight::FlightCallOptions>`. This
allows clients to set a timeout on calls or provide custom HTTP
headers, among other features. Also, some objects returned by client
RPC calls expose a ``Cancel`` method which allows terminating a call
early.

On the server side, no additional code is needed to implement
timeouts. For cancellation, the server needs to manually poll
:func:`ServerCallContext::is_cancelled
<arrow::flight::ServerCallContext::is_cancelled>` to check if the
client has cancelled the call, and if so, break out of any processing
the server is currently doing.

Enabling TLS
============

TLS can be enabled when setting up a server by providing a certificate
and key pair to :func:`FlightServerBase::Init
<arrow::flight::FlightServerBase::Init>`.

On the client side, use :func:`Location::ForGrpcTls
<arrow::flight::Location::ForGrpcTls>` to construct the
:class:`arrow::flight::Location` to listen on.

Enabling Authentication
=======================

.. warning:: Authentication is insecure without enabling TLS.

Handshake-based authentication can be enabled by implementing
:class:`ServerAuthHandler <arrow::flight::ServerAuthHandler>` and
providing this to the server during construction.

Authentication consists of two parts: on initial client connection,
the server and client authentication implementations can perform any
negotiation needed. The client authentication handler then provides a
token that will be attached to future calls. This is done by calling
:func:`Authenticate <arrow::flight::FlightClient::Authenticate>` with
the desired client authentication implementation.

On each RPC thereafter, the client handler's token is automatically
added to the call in the request headers. The server authentication
handler validates the token and provides the identity of the
client. On the server, this identity can be obtained from the
:class:`arrow::flight::ServerCallContext`.

Custom Middleware
=================

Servers and clients support custom middleware (or interceptors) that
are called on every request and can modify the request in a limited
fashion.  These can be implemented by subclassing :class:`ServerMiddleware
<arrow::flight::ServerMiddleware>` and :class:`ClientMiddleware
<arrow::flight::ClientMiddleware>`, then providing them when creating
the client or server.

Middleware are fairly limited, but they can add headers to a
request/response. On the server, they can inspect incoming headers and
fail the request; hence, they can be used to implement custom
authentication methods.

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
