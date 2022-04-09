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

Arrow Flight is a development framework to offer to us the building blocks
to implement applications base on our needs.

In this journey, you need to implement how do you are planning to expose
your business operations in base of RPC Methods and Request Patterns
provided by Flight.

For Flight Server side operations you need to implement FlightProducer interface
methods.

.. code-block:: Java

    public class TutorialFlightProducer implements FlightProducer {
        @Override
        // Override methods or use NoOpFlightProducer for only methods needed
    }

Also you need to specify where to listen for, let's combine producer and location
to create and start the Flight Server. This will start the server, but won't block
the rest of the program. Call `FlightServer.awaitTermination` to block until the
server stops.

.. code-block:: Java

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.ActionType;
    import org.apache.arrow.flight.Criteria;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightInfo;
    import org.apache.arrow.flight.FlightProducer;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.PutResult;
    import org.apache.arrow.flight.Result;
    import org.apache.arrow.flight.SchemaResult;
    import org.apache.arrow.flight.Ticket;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import org.apache.arrow.util.AutoCloseables;

    class TutorialFlightProducer implements FlightProducer {
        @Override
        // Override methods or use NoOpFlightProducer for only methods needed
    }

    Location location = Location.forGrpcInsecure("0.0.0.0", 0);

    try(
        BufferAllocator allocator = new RootAllocator();
        FlightServer tutorialFlightServer = FlightServer.builder(
                allocator,
                location,
                new TutorialFlightProducer()
        ).build();
    ){
        tutorialFlightServer.start();
        System.out.println("Server listening on port " + tutorialFlightServer.getPort());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nExiting command...");
                AutoCloseables.close(tutorialFlightServer, allocator);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
        tutorialFlightServer.awaitTermination();
    } catch (Exception e) {
        e.printStackTrace();
    }

.. code-block:: shell

    Server listening on port 58104

Using the Flight Client
=======================

To connect to a Flight service, call `FlightClient.builder` with a location.

.. code-block:: Java

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    Location location = Location.forGrpcInsecure("0.0.0.0", 58104);

    try(BufferAllocator allocator = new RootAllocator();
        FlightClient tutorialFlightClient = FlightClient.builder(allocator, location).build()){
        // ... Consume operations exposed by Flight Server
    } catch (Exception e) {
        e.printStackTrace();
    }

Cancellation and Timeouts
=========================

When making a call, clients can optionally provide `CallOptions`. This allows
clients to set a timeout on calls. Also, some objects returned by client RPC calls
expose a cancel method which allows terminating a call early.

.. code-block:: Java

    import org.apache.arrow.flight.Action;
    import org.apache.arrow.flight.CallOptions;
    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.Result;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    import java.util.Iterator;
    import java.util.concurrent.TimeUnit;

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

    import org.apache.arrow.flight.FlightClient;
    import org.apache.arrow.flight.FlightDescriptor;
    import org.apache.arrow.flight.FlightStream;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.Ticket;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;
    import java.nio.charset.StandardCharsets;

    // Client
    Location location = Location.forGrpcInsecure("0.0.0.0", 58609);
    try(BufferAllocator allocator = new RootAllocator();
        FlightClient tutorialFlightClient = FlightClient.builder(allocator, location).build()){
        try(FlightStream flightStream = tutorialFlightClient.getStream(new Ticket(
                FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
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

TLS can be enabled when setting up a server by providing a certificate and key pair to `FlightServer.builder.useTls`.

On the client side, use `FlightClient.builder.trustedCertificates`.

Enabling Authentication
=======================



Custom Middleware
=================

Servers and clients support custom middleware (or interceptors) that are called on every
request and can modify the request in a limited fashion. These can be implemented by implements
`FlightServerMiddleware` and `FlightClientMiddleware` interface methods.

Middleware are fairly limited, but they can add headers to a request/response.

Example: Need to intercept Flight server headers at request and print values also for the response
headers add a key-value and print values.

.. code-block:: Java

    import org.apache.arrow.flight.CallHeaders;
    import org.apache.arrow.flight.CallInfo;
    import org.apache.arrow.flight.CallStatus;
    import org.apache.arrow.flight.FlightServer;
    import org.apache.arrow.flight.FlightServerMiddleware;
    import org.apache.arrow.flight.Location;
    import org.apache.arrow.flight.RequestContext;
    import org.apache.arrow.memory.RootAllocator;

    import java.io.IOException;
    import java.util.Collections;
    import java.util.List;

    class ServerMiddlewarePair<T extends FlightServerMiddleware> {
        final FlightServerMiddleware.Key<T> key;
        final FlightServerMiddleware.Factory<T> factory;
        ServerMiddlewarePair(FlightServerMiddleware.Key<T> key, FlightServerMiddleware.Factory<T> factory) {
            this.key = key;
            this.factory = factory;
        }
    }
    class HeadersAnalyzerServerMiddleware implements FlightServerMiddleware {
        @Override
        public void onBeforeSendingHeaders(CallHeaders outgoingHeaders) {
            System.out.println("OUT Headers: onBeforeSendingHeaders");
            outgoingHeaders.insert("new-key-response", "new-value-response");
            System.out.println(outgoingHeaders);
        }
        @Override
        public void onCallCompleted(CallStatus status) {
        }
        @Override
        public void onCallErrored(Throwable err) {
        }
        class Factory implements FlightServerMiddleware.Factory<HeadersAnalyzerServerMiddleware> {
            HeadersAnalyzerServerMiddleware instance = new HeadersAnalyzerServerMiddleware();
            @Override
            public HeadersAnalyzerServerMiddleware onCallStarted(CallInfo info, CallHeaders incomingHeaders, RequestContext context) {
                System.out.println("IN Headers: onCallStarted");
                System.out.println(incomingHeaders);
                return instance;
            }
        }
    }
    Location location = Location.forGrpcInsecure("0.0.0.0", 33333);
    try (RootAllocator allocator = new RootAllocator(Long.MAX_VALUE)){
        HeadersAnalyzerServerMiddleware headers = new HeadersAnalyzerServerMiddleware();
        HeadersAnalyzerServerMiddleware.Factory factory = headers.new Factory();
        final List<ServerMiddlewarePair<HeadersAnalyzerServerMiddleware>> middleware = Collections
                .singletonList(new ServerMiddlewarePair<>(FlightServerMiddleware.Key.of("m"), factory));
        final FlightServer.Builder builder = FlightServer.builder(
                allocator, location, new CookbookProducer(allocator, location));
        middleware.forEach(pair -> builder.middleware(pair.key, pair.factory));
        try(FlightServer flightServer = builder.build()) {
            try {
                flightServer.start();
            } catch (IOException e) {
                System.exit(1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

Any request to Flight Server services should print these messages:

.. code-block:: shell

    IN Headers: onCallStarted
    Metadata(content-type=application/grpc,user-agent=grpc-java-netty/1.44.1,grpc-accept-encoding=gzip)
    OUT Headers: onBeforeSendingHeaders
    Metadata(new-key-response=new-value-response)