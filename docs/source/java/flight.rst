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

================
Arrow Flight RPC
================

Arrow Flight is an RPC framework for efficient transfer of Arrow data
over the network.

.. contents::

.. seealso::

   :doc:`Flight protocol documentation <../format/Flight>`
        Documentation of the Flight protocol, including how to use
        Flight conceptually.

   `Java Cookbook <https://arrow.apache.org/cookbook/java/flight.html>`_
        Recipes for using Arrow Flight in Java.

Writing a Flight Service
========================

Flight servers implement the `FlightProducer`_ interface. For convenience,
they can subclass `NoOpFlightProducer`_ instead, which offers default
implementations of all the RPC methods.

.. code-block:: Java

    public class TutorialFlightProducer implements FlightProducer {
        @Override
        // Override methods or use NoOpFlightProducer for only methods needed
    }

Each RPC method always takes a ``CallContext`` for common parameters. To indicate
failure, pass an exception to the "listener" if present, or else raise an
exception.

.. code-block:: Java

    // Server
    @Override
    public void listFlights(CallContext context, Criteria criteria, StreamListener<FlightInfo> listener) {
        // ...
        listener.onError(
            CallStatus.UNAUTHENTICATED.withDescription(
                "Custom UNAUTHENTICATED description message.").toRuntimeException());
        // ...
    }

    // Client
    try{
        Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
        // ...
    } catch (FlightRuntimeException e){
        // Catch UNAUTHENTICATED exception
    }

To start a server, create a `Location`_ to specify where to listen, and then create
a `FlightServer`_ with an instance of a producer. This will start the server, but
won't block the rest of the program. Call ``FlightServer.awaitTermination``
to block until the server stops.

.. code-block:: Java

    class TutorialFlightProducer implements FlightProducer {
        @Override
        // Override methods or use NoOpFlightProducer for only methods needed
    }

    Location location = Location.forGrpcInsecure("0.0.0.0", 0);
    try(
        BufferAllocator allocator = new RootAllocator();
        FlightServer server = FlightServer.builder(
                allocator,
                location,
                new TutorialFlightProducer()
        ).build();
    ){
        server.start();
        System.out.println("Server listening on port " + server.getPort());
        server.awaitTermination();
    } catch (Exception e) {
        e.printStackTrace();
    }

.. code-block:: shell

    Server listening on port 58104

Using the Flight Client
=======================

To connect to a Flight service, create a `FlightClient`_ with a location.

.. code-block:: Java

    Location location = Location.forGrpcInsecure("0.0.0.0", 58104);

    try(BufferAllocator allocator = new RootAllocator();
        FlightClient client = FlightClient.builder(allocator, location).build()){
        // ... Consume operations exposed by Flight server
    } catch (Exception e) {
        e.printStackTrace();
    }

Cancellation and Timeouts
=========================

When making a call, clients can optionally provide ``CallOptions``. This allows
clients to set a timeout on calls. Also, some objects returned by client RPC calls
expose a cancel method which allows terminating a call early.

.. code-block:: Java

    Location location = Location.forGrpcInsecure("0.0.0.0", 58609);

    try(BufferAllocator allocator = new RootAllocator();
        FlightClient tutorialFlightClient = FlightClient.builder(allocator, location).build()){

        Iterator<Result> resultIterator = tutorialFlightClient.doAction(
                new Action("test-timeout"),
                CallOptions.timeout(2, TimeUnit.SECONDS)
        );
    } catch (Exception e) {
        e.printStackTrace();
    }

On the server side, timeouts are transparent. For cancellation, the server needs to manually poll
``setOnCancelHandler`` or ``isCancelled`` to check if the client has cancelled the call,
and if so, break out of any processing the server is currently doing.

.. code-block:: Java

    // Client
    Location location = Location.forGrpcInsecure("0.0.0.0", 58609);
    try(BufferAllocator allocator = new RootAllocator();
        FlightClient tutorialFlightClient = FlightClient.builder(allocator, location).build()){
        try(FlightStream flightStream = flightClient.getStream(new Ticket(new byte[]{}))) {
            // ...
            flightStream.cancel("tutorial-cancel", new Exception("Testing cancellation option!"));
        }
    } catch (Exception e) {
        e.printStackTrace();
    }
    // Server
    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
        // ...
        listener.setOnCancelHandler(()->{
                    // Implement logic to handle cancellation option
                });
    }

Enabling TLS
============

TLS can be enabled when setting up a server by providing a
certificate and key pair to ``FlightServer.Builder.useTls``.

On the client side, use ``Location.forGrpcTls`` to create the Location for the client.

Enabling Authentication
=======================

.. warning:: Authentication is insecure without enabling TLS.

Handshake-based authentication can be enabled by implementing
``ServerAuthHandler``. Authentication consists of two parts: on
initial client connection, the server and client authentication
implementations can perform any negotiation needed. The client authentication
handler then provides a token that will be attached to future calls. 

The client send data to be validated through ``ClientAuthHandler.authenticate``
The server validate data received through ``ServerAuthHandler.authenticate``.

Custom Middleware
=================

Servers and clients support custom middleware (or interceptors) that are called on every
request and can modify the request in a limited fashion. These can be implemented by implementing the
``FlightServerMiddleware`` and ``FlightClientMiddleware`` interfaces.

Middleware are fairly limited, but they can add headers to a
request/response. On the server, they can inspect incoming headers and
fail the request; hence, they can be used to implement custom
authentication methods.

Adding Services
===============

Servers can add other gRPC services. For example, to add the `Health Check service <https://github.com/grpc/grpc/blob/master/doc/health-checking.md>`_:

.. code-block:: Java

    final HealthStatusManager statusManager = new HealthStatusManager();
    final Consumer<NettyServerBuilder> consumer = (builder) -> {
      builder.addService(statusManager.getHealthService());
    };
    final Location location = forGrpcInsecure(LOCALHOST, 5555);
    try (
        BufferAllocator a = new RootAllocator(Long.MAX_VALUE);
        Producer producer = new Producer(a);
        FlightServer s = FlightServer.builder(a, location, producer)
            .transportHint("grpc.builderConsumer", consumer).build().start();
    ) {
      Channel channel = NettyChannelBuilder.forAddress(location.toSocketAddress()).usePlaintext().build();
      HealthCheckResponse response = HealthGrpc
              .newBlockingStub(channel)
              .check(HealthCheckRequest.getDefaultInstance());

      System.out.println(response.getStatus());
    }


:ref:`Flight best practices <flight-best-practices>`
====================================================


.. _`FlightClient`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightClient.html
.. _`FlightProducer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightProducer.html
.. _`FlightServer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightServer.html
.. _`NoOpFlightProducer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/NoOpFlightProducer.html
.. _`Location`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/Location.html
