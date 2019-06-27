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

Arrow Flight RPC
================

Arrow Flight is a RPC framework for high-performance data services
based on Arrow data, and is built on top of gRPC_ and the :doc:`IPC
format <IPC>`.

Flight is organized around streams of Arrow record batches, being
either downloaded from or uploaded to another service. A set of
metadata methods offers discovery and introspection of streams, as
well as the ability to implement application-specific methods.

Methods and message wire formats are defined by Protobuf, enabling
interoperability with clients that may support gRPC and Arrow
separately, but not Flight. However, Flight implementations include
further optimizations to avoid overhead in usage of Protobuf (mostly
around avoiding excessive memory copies).

.. _gRPC: https://grpc.io/

RPC Methods
-----------

Flight defines a set of RPC methods for uploading/downloading data,
retrieving metadata about a data stream, listing available data
streams, and for implementing application-specific RPC methods. A
Flight service implements some subset of these methods, while a Flight
client can call any of these methods. Thus, one Flight client can
connect to any Flight service and perform basic operations.

Data streams are identified by descriptors, which are either a path or
an arbitrary binary command. A client that wishes to download the data
would:

#. Construct or acquire a ``FlightDescriptor`` for the data set they
   are interested in. A client may know what descriptor they want
   already, or they may use methods like ``ListFlights`` to discover
   them.
#. Call ``GetFlightInfo(FlightDescriptor)`` to get a ``FlightInfo``
   message containing details on where the data is located (as well as
   other metadata, like the schema and possibly an estimate of the
   dataset size).

   Flight does not require that data live on the same server as
   metadata: this call may list other servers to connect to. The
   ``FlightInfo`` message includes a ``Ticket``, an opaque binary
   token that the server uses to identify the exact data set being
   requested.
#. Connect to other servers (if needed).
#. Call ``DoGet(Ticket)`` to get back a stream of Arrow record
   batches.

To upload data, a client would:

#. Construct or acquire a ``FlightDescriptor``, as before.
#. Call ``DoPut(FlightData)`` and upload a stream of Arrow record
   batches. They would also include the ``FlightDescriptor`` with the
   first message.

See `Protocol Buffer Definitions`_ for full details on the methods and
messages involved.

Authentication
~~~~~~~~~~~~~~

Flight supports application-implemented authentication
methods. Authentication, if enabled, has two phases: at connection
time, the client and server can exchange any number of messages. Then,
the client can provide a token alongside each call, and the server can
validate that token.

Applications may use any part of this; for instance, they may ignore
the initial handshake and send an externally acquired token on each
call, or they may establish trust during the handshake and not
validate a token for each call. (Note that the latter is not secure if
you choose to deploy a layer 7 load balancer, as is common with gRPC.)

External Resources
------------------

- https://arrow.apache.org/blog/2018/10/09/0.11.0-release/
- https://www.slideshare.net/JacquesNadeau5/apache-arrow-flight-overview

Protocol Buffer Definitions
---------------------------

.. literalinclude:: ../../../format/Flight.proto
   :language: protobuf
   :linenos:
