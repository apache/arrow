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

.. currentmodule:: pyarrow.flight
.. highlight:: python

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
        Python API documentation listing all of the various client and
        server classes.

   `Python Cookbook <https://arrow.apache.org/cookbook/py/flight.html>`_
        Recipes for using Arrow Flight in Python.

Writing a Flight Service
========================

Servers are subclasses of :class:`FlightServerBase`. To implement
individual RPCs, override the RPC methods on this class.

.. code-block:: python

   import pyarrow.flight as flight

   class MyFlightServer(flight.FlightServerBase):
       def list_flights(self, context, criteria):
           info = flight.FlightInfo(...)
           yield info

Each RPC method always takes a :class:`ServerCallContext` for common
parameters. To indicate failure, raise an exception; Flight-specific
errors can be indicated by raising one of the subclasses of
:class:`FlightError`.

To start a server, create a :class:`Location` to specify where to
listen, and create an instance of the server. (A string will be
converted into a location.) This will start the server, but won't
block the rest of the program. Call :meth:`FlightServerBase.serve` to
block until the server stops.

.. code-block:: python

   # Listen to all interfaces on a free port
   server = MyFlightServer("grpc://0.0.0.0:0")

   print("Server listening on port", server.port)
   server.serve()

Using the Flight Client
=======================

To connect to a Flight service, call :meth:`pyarrow.flight.connect`
with a location.

Cancellation and Timeouts
=========================

When making a call, clients can optionally provide
:class:`FlightCallOptions`. This allows clients to set a timeout on
calls or provide custom HTTP headers, among other features. Also, some
objects returned by client RPC calls expose a ``cancel`` method which
allows terminating a call early.

On the server side, timeouts are transparent. For cancellation, the
server needs to manually poll :meth:`ServerCallContext.is_cancelled`
to check if the client has cancelled the call, and if so, break out of
any processing the server is currently doing.

Enabling TLS
============

TLS can be enabled when setting up a server by providing a certificate
and key pair to :class:`FlightServerBase`.

On the client side, use :func:`Location.for_grpc_tls` to construct the
:class:`Location` to listen on.

Enabling Authentication
=======================

.. warning:: Authentication is insecure without enabling TLS.

Handshake-based authentication can be enabled by implementing
:class:`ServerAuthHandler`. Authentication consists of two parts: on
initial client connection, the server and client authentication
implementations can perform any negotiation needed; then, on each RPC
thereafter, the client provides a token. The server authentication
handler validates the token and provides the identity of the
client. This identity can be obtained from the
:class:`ServerCallContext`.

Custom Middleware
=================

Servers and clients support custom middleware (or interceptors) that
are called on every request and can modify the request in a limited
fashion.  These can be implemented by subclassing
:class:`ServerMiddleware` and :class:`ClientMiddleware`, then
providing them when creating the client or server.

Middleware are fairly limited, but they can add headers to a
request/response. On the server, they can inspect incoming headers and
fail the request; hence, they can be used to implement custom
authentication methods.

:ref:`Flight best practices <flight-best-practices>`
====================================================
