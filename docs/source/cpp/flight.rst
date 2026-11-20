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

.. _cpp-flight-async-server:

Writing an Asynchronous Flight Service
======================================

A :class:`arrow::flight::FlightServerBase` handler holds a gRPC thread until
it returns. When the work behind an RPC waits on something else (another
service, a query engine, storage), derive from
:class:`arrow::flight::AsyncGenericFlightServerBase` instead: every handler
returns an :class:`arrow::Future`, and no gRPC thread waits for it.

.. note:: The asynchronous server needs Arrow to be built against gRPC 1.65
          or later. With an older gRPC,
          :func:`arrow::flight::AsyncGenericFlightServerBase::Init` returns
          ``NotImplemented``.

.. code-block:: cpp

   class MyAsyncServer : public arrow::flight::AsyncGenericFlightServerBase {
    public:
     explicit MyAsyncServer(std::shared_ptr<Catalog> catalog)
         : catalog_(std::move(catalog)) {}

     arrow::Future<std::shared_ptr<arrow::flight::FlightInfo>> GetFlightInfoAsync(
         const arrow::flight::ServerCallContext& context,
         const arrow::flight::FlightDescriptor& request) override {
       // `request` is only valid during this call: hand on a copy.
       return catalog_->LookUp(request);
     }

    private:
     std::shared_ptr<Catalog> catalog_;
   };

Start, serve and stop the server exactly like a synchronous one:

.. code-block:: cpp

   MyAsyncServer server(catalog);
   ARROW_ASSIGN_OR_RAISE(auto location,
                         arrow::flight::Location::ForGrpcTcp("0.0.0.0", 0));
   ARROW_RETURN_NOT_OK(server.Init(arrow::flight::FlightServerOptions(location)));
   ARROW_RETURN_NOT_OK(server.SetShutdownOnSignals({SIGTERM}));
   ARROW_RETURN_NOT_OK(server.Serve());

Handlers you do not override answer ``NotImplemented``. Each request argument
(:class:`arrow::flight::Ticket`, :class:`arrow::flight::FlightDescriptor`,
:class:`arrow::flight::Criteria`, :class:`arrow::flight::Action`) and the
:class:`arrow::flight::ServerCallContext` are only valid during the handler
call. Authentication goes through ``Handshake()`` and ``ValidateToken()``: a
blocking :class:`arrow::flight::ServerAuthHandler` in the server options is
rejected by ``Init()``. Server middleware is supported.

Reacting to Cancellation
------------------------

When the client cancels, its deadline expires, or the connection drops, the
transport finishes the RPC without waiting for the handler's pending future,
and calls ``Close()`` on a DoGet stream. To stop the work behind a pending
future promptly instead of polling
:func:`arrow::flight::ServerCallContext::is_cancelled`, register a callback
with :func:`arrow::flight::AsyncGenericFlightServerBase::AddCancelCallback`:

.. code-block:: cpp

   arrow::Future<std::shared_ptr<arrow::flight::AsyncFlightDataStream>> DoGetAsync(
       const arrow::flight::ServerCallContext& context,
       const arrow::flight::Ticket& request) override {
     std::shared_ptr<Query> query = catalog_->Start(request);
     ARROW_RETURN_NOT_OK(AddCancelCallback(context, [query] { query->Cancel(); }));
     return query->Ready().Then(
         [query]() -> std::shared_ptr<arrow::flight::AsyncFlightDataStream> {
           return std::make_shared<QueryStream>(query);
         });
   }

The callback runs once, on the transport thread that sees the cancellation,
after the RPC's status is settled. If the RPC is already cancelled, it runs
before ``AddCancelCallback()`` returns. It never runs when the RPC ends
normally. It must not block.

Threading
---------

The transport has no thread or executor of its own. Its continuation on each
future a handler, stream or listener returns runs on whichever thread
completes that future, or inline on the gRPC thread that called you if the
future is already complete. That continuation issues the RPC's next
operation: the next write, the next read, or the end of the RPC. You choose
the thread that issues the RPC's I/O by choosing where you complete your
futures:

.. code-block:: cpp

   arrow::Future<std::optional<arrow::flight::FlightPayload>> NextAsync() override {
     // Produced on io_executor_, so the next write is issued from that thread.
     return arrow::DeferNotOk(io_executor_->Submit([this] { return ProduceNext(); }));
   }

Complete futures promptly and never block inside a handler: a blocked gRPC
callback thread stalls every other RPC served by it.

The internals of the transport are described in
:ref:`cpp-flight-async-internals`.

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

.. _flight-best-practices:

Best practices
==============

gRPC
----

When using the default gRPC transport, options can be passed to it via
:member:`arrow::flight::FlightClientOptions::generic_options`. For example:

.. tab-set::

   .. tab-item:: C++

      .. code-block:: cpp

         auto options = FlightClientOptions::Defaults();
         // Set the period after which a keepalive ping is sent on transport.
         options.generic_options.emplace_back(GRPC_ARG_KEEPALIVE_TIME_MS, 60000);

   .. tab-item:: Python

      .. code-block:: python

         # Set the period after which a keepalive ping is sent on transport.
         generic_options = [("GRPC_ARG_KEEPALIVE_TIME_MS", 60000)]
         client = pyarrow.flight.FlightClient(server_uri, generic_options=generic_options)

Also see `best gRPC practices`_ and available `gRPC keys`_.

Re-use clients whenever possible
--------------------------------

Creating and closing clients requires setup and teardown on the client and
server side which can take away from actually handling RPCs. Reuse clients
whenever possible to avoid this. Note that clients are thread-safe, so a
single client can be shared across multiple threads.

Don’t round-robin load balance
------------------------------

`Round robin load balancing`_ means every client can have an open connection to
every server, causing an unexpected number of open connections and depleting
server resources.

Debugging connection issues
---------------------------

When facing unexpected disconnects on long running connections use netstat to
monitor the number of open connections. If number of connections is much
greater than the number of clients it might cause issues.

For debugging, certain environment variables enable logging in gRPC. For
example, ``env GRPC_VERBOSITY=info GRPC_TRACE=http`` will print the initial
headers (on both sides) so you can see if gRPC established the connection or
not. It will also print when a message is sent, so you can tell if the
connection is open or not.

gRPC may not report connection errors until a call is actually made.
Hence, to detect connection errors when creating a client, some sort
of dummy RPC should be made.

Memory management
-----------------

Flight tries to reuse allocations made by gRPC to avoid redundant
data copies. However, experience shows that such data is frequently
misaligned. Some use cases might require data to have data type-specific
alignment (for example, for the data buffer of an Int32 array to be aligned
on a 4-byte boundary), which can be enforced
by setting :member:`arrow::ipc::IpcReadOptions::ensure_alignment`
to :member:`arrow::ipc::Alignment::kDataTypeSpecificAlignment`.
This uses the :member:`arrow::ipc::IpcReadOptions::memory_pool`
to allocate memory with aligned addresses, but only for mis-aligned data.
However, this creates data copies of your data received via Flight.

Unless gRPC data are copied as described above, allocations made by gRPC may not
be tracked by the Arrow memory pool, and that memory usage behavior,
such as whether free memory is returned to the system, is dependent
on the allocator that gRPC uses (usually the system allocator).

A quick way of testing: attach to the process with a debugger and call
``malloc_trim``, or call :func:`ReleaseUnused <arrow::MemoryPool::ReleaseUnused>`
on the system pool. If memory usage drops, then likely, there is memory
allocated by gRPC or by the application that the system allocator was holding
on to. This can be adjusted in platform-specific ways; see an investigation
in ARROW-16697_ for an example of how this works on Linux/glibc. glibc malloc
can be explicitly told to dump caches.

Excessive traffic
-----------------

gRPC will spawn up to max threads quota of threads for concurrent clients. Those
threads are not necessarily cleaned up (a "cached thread pool" in Java parlance).
glibc malloc clears some per thread state and the default tuning never clears
caches in some workloads.

gRPC's default behaviour allows one server to accept many connections from many
different clients, but if requests do a lot of work (as they may under Flight),
the server may not be able to keep up. Configuring clients to retry
with backoff (and potentially connect to a different node), would give more
consistent quality of service.

.. tab-set::

   .. tab-item:: C++

      .. code-block:: cpp

         auto options = FlightClientOptions::Defaults();
         // Set the minimum time between subsequent connection attempts.
         options.generic_options.emplace_back(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 2000);

   .. tab-item:: Python

      .. code-block:: python

         # Set the minimum time between subsequent connection attempts.
         generic_options = [("GRPC_ARG_MIN_RECONNECT_BACKOFF_MS", 2000)]
         client = pyarrow.flight.FlightClient(server_uri, generic_options=generic_options)


Limiting DoPut Batch Size
--------------------------

You may wish to limit the maximum batch size a client can submit to a server through
DoPut, to prevent a request from taking up too much memory on the server. On
the client-side, set :member:`arrow::flight::FlightClientOptions::write_size_limit_bytes`.
On the server-side, set the gRPC option ``GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH``.
The client-side option will return an error that can be retried with smaller batches,
while the server-side limit will close out the connection. Setting both can be wise, since
the former provides a better user experience but the latter may be necessary to defend
against impolite clients.

Closing unresponsive connections
--------------------------------

1. A stale call can be closed using
   :member:`arrow::flight::FlightCallOptions::stop_token`. This requires recording the
   stop token at call establishment time.

   .. tab-set::

      .. tab-item:: C++

         .. code-block:: cpp

              StopSource stop_source;
              FlightCallOptions options;
              options.stop_token = stop_source.token();
              stop_source.RequestStop(Status::Cancelled("StopSource"));
              flight_client->DoAction(options, {});


2. Use call timeouts. (This is a general gRPC best practice.)

   .. tab-set::

      .. tab-item:: C++

         .. code-block:: cpp

            FlightCallOptions options;
            options.timeout = TimeoutDuration{0.2};
            Status status = client->GetFlightInfo(options, FlightDescriptor{}).status();

      .. tab-item:: Java

         .. code-block:: java

            Iterator<Result> results = client.doAction(new Action("hang"), CallOptions.timeout(0.2, TimeUnit.SECONDS));

      .. tab-item:: Python

         .. code-block:: python

            options = pyarrow.flight.FlightCallOptions(timeout=0.2)
            result = client.do_action(action, options=options)


3. Client timeouts are not great for long-running streaming calls, where it may
   be hard to choose a timeout for the entire operation. Instead, what is often
   desired is a per-read or per-write timeout so that the operation fails if it
   isn't making progress. This can be implemented with a background thread that
   calls Cancel() on a timer, with the main thread resetting the timer every time
   an operation completes successfully. For a fully-worked out example, see the
   Cookbook.

   .. note:: There is a long standing ticket for a per-write/per-read timeout
             instead of a per call timeout (ARROW-6062_), but this is not (easily)
             possible to implement with the blocking gRPC API.

.. _best gRPC practices: https://grpc.io/docs/guides/performance/#general
.. _gRPC keys: https://grpc.github.io/grpc/cpp/group__grpc__arg__keys.html
.. _Round robin load balancing: https://github.com/grpc/grpc/blob/master/doc/load-balancing.md#round_robin
.. _ARROW-15764: https://issues.apache.org/jira/browse/ARROW-15764
.. _ARROW-16697: https://issues.apache.org/jira/browse/ARROW-16697
.. _ARROW-6062: https://issues.apache.org/jira/browse/ARROW-6062
.. _gRPC: https://grpc.io/
