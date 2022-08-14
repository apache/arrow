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

Best practices
==============

gRPC
----

When using default gRPC transport options can be passed to it via
:func:`arrow::flight::FlightClientOptions.generic_options`. For example:

.. code-block:: cpp

   auto options = FlightClientOptions::Defaults();
   // Set a very low limit at the gRPC layer to fail all calls
   options.generic_options.emplace_back(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, 4);

See available `gRPC keys_`.
Best gRPC practices are described here_.


Re-use clients whenever possible


Closing clients causes gRPC to close and clean up connections which can take
several seconds per connection. This will stall server and client threads if
done too frequently. Client reuse will avoid this issue.

Don’t round-robin load balance
------------------------------

Round robin balancing can cause every client to have an open connection to
every server causing an unexpected number of open connections and a depletion
of resources.

Debugging
---------

Use netstat to see the number of open connections.
For debug - env GRPC_VERBOSITY=info GRPC_TRACE=http will print the initial
headers (on both sides) so you can see if grpc established the connection or
not. It will also print when a message is sent, so you can tell if the
connection is open or not.
Note: "connect" isn't really a connect and we’ve observed that gRPC does not
give you the actual error until you first try to make a call. This can cause
error being reported at unexpected times.

Use ListFlights sparingly
-------------------------

ListFlights endpoint is largely just implemented as a normal GRPC stream
endpoint and can hit transfer bottlenecks if used too much. To estimate data
transfer bottleneck:
5k schemas will serialize to about 1-5 MB/call. Assuming a gRPC localhost
bottleneck of 3GB/s you can at best serve 600-3000 clients/s.

ARROW-15764_ proposes a caching optimisation for server side, but it was not
yet implemented.


Memory cache client-side
------------------------

Flight uses gRPC allocator wherever possible.

gRPC will spawn an unbounded number of threads for concurrent clients. Those
threads are not necessarily cleaned up (cached thread pool in java parlance).
glibc malloc clears some per thread state and the default tuning never clears
caches in some workloads. But you can explicitly tell malloc to dump caches.
See ARROW-16697_ as an example.

A quick way of testing: attach to the process with a debugger and call malloc_trim


Excessive traffic
-----------------

There are basically two ways to handle excessive traffic:
* unbounded thread pool -> everyone gets serviced, but it might take forever.
This is what you are seeing now.
bounded thread pool -> Reject connections / requests when under load, and have
clients retry with backoff. This also gives an opportunity to retry with a
different node. Not everyone gets serviced but quality of service stays consistent.

Closing unresponsive connections
--------------------------------

* A stale connection can be closed using FlightCallOptions.stop_token. This requires
recording the stop token at connection establishment time.
* Use client timeout
* here is a long standing ticket for a per-write/per-read timeout instead of a per
call timeout (ARROW-6062_), but this is not (easily) possible to implement with the
blocking gRPC API. For now one can also do something like set up a background thread
that calls cancel() on a timer and have the main thread reset the timer every time a
write operation completes successfully (that means one needs to use to_batches() +
write_batch and not write_table).

.. _here: https://grpc.io/docs/guides/performance/#general
.. _gRPC keys: https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html
.. _ARROW-15764: https://issues.apache.org/jira/browse/ARROW-15764
.. _ARROW-16697: https://issues.apache.org/jira/browse/ARROW-16697
.. _ARROW-6062: https://issues.apache.org/jira/browse/ARROW-6062


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
