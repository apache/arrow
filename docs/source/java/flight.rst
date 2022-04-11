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

Arrow Flight is an RPC framework for efficient transfer of Flight data
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

Each RPC method always takes a `CallStatus` for common parameters. To indicate
failure, raise an exception.

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

To connect to a Flight service, call `FlightClient`_ with a location.

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

When making a call, clients can optionally provide `CallOptions`. This allows
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
`setOnCancelHandler` or `isCancelled` to check if the client has cancelled the call, and if so,
break out of any processing the server is currently doing.

.. code-block:: Java

    // Client
    Location location = Location.forGrpcInsecure("0.0.0.0", 58609);
    try(BufferAllocator allocator = new RootAllocator();
        FlightClient tutorialFlightClient = FlightClient.builder(allocator, location).build()){
        try(FlightStream flightStream = flightClient.getStream(new Ticket(new byte[]{}))) {
            // ...
            flightStream.cancel("tutorial-cancel", new Exception("Testing cancellation opion!"));
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
certificate and key pair to `FlightServer.builder.useTls`.

On the client side, use `FlightClient.builder.trustedCertificates`.

Enabling Authentication
=======================

.. warning:: Authentication is insecure without enabling TLS.

Handshake-based authentication can be enabled by implementing
`ServerAuthHandler`. Authentication consists of two parts: on
initial client connection, the server and client authentication
implementations can perform any negotiation needed; then, on each RPC
thereafter, the client provides a token. The server authentication
handler validates the token and provides the identity of the
client. This identity can be obtained from the
`CallContext.peerIdentity`.

.. code-block:: Java

    // Client
    FlightClient client = FlightClient.builder().build();
    client.authenticateBasic("user", "password");
    // Server
    FlightServer server = FlightServer.builder().authHandler(new BasicServerAuthHandler(validator)).build();
    // CallHeaders printed on the Flight Server:
    Metadata(content-type=application/grpc,user-agent=grpc-java-netty/1.44.1,auth-token-bin=bXlfdG9rZW4,grpc-accept-encoding=gzip)



Custom Middleware
=================

Servers and clients support custom middleware (or interceptors) that are called on every
request and can modify the request in a limited fashion. These can be implemented by implementing the
`FlightServerMiddleware` and `FlightClientMiddleware` interfaces.

Middleware are fairly limited, but they can add headers to a request/response.

.. _`FlightClient`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightClient.html
.. _`FlightProducer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightProducer.html
.. _`FlightServer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/FlightServer.html
.. _`NoOpFlightProducer`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/NoOpFlightProducer.html
.. _`Location`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/flight/Location.html
