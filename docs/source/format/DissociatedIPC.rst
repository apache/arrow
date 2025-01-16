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

.. _dissociated-ipc:

========================
Dissociated IPC Protocol
========================

.. warning::

    Experimental: The Dissociated IPC Protocol is experimental in its current
    form. Based on feedback and usage the protocol definition may change until
    it is fully standardized.

Rationale
=========

The :ref:`Arrow IPC format <format-ipc>` describes a protocol for transferring
Arrow data as a stream of record batches. This protocol expects a continuous
stream of bytes divided into discrete messages (using a length prefix and
continuation indicator). Each discrete message consists of two portions:

* A `Flatbuffers`_ header message
* A series of bytes consisting of the flattened and packed body buffers (some
  message types, like Schema messages, do not have this section)
  - This is referred to as the *message body* in the IPC format spec.

For most cases, the existing IPC format as it currently exists is sufficiently efficient:

* Receiving data in the IPC format allows zero-copy utilization of the body
  buffer bytes, no deserialization is required to form Arrow Arrays
* An IPC file format can be memory-mapped because it is location agnostic
  and the bytes of the file are exactly what is expected in memory.

However, there are use cases that aren't handled by this:

* Constructing the IPC record batch message requires allocating a contiguous
  chunk of bytes and copying all of the data buffers into it, packed together
  back-to-back. This pessimizes the common case of wrapping existing, directly
  consumable data into an IPC message.
* Even if Arrow data is located in a memory accessible across process boundaries
  or transports (such as UCX), there is no standard way to specify that shared
  location to consumers which could take advantage of it.
* Arrow data located on a non-CPU device (such as a GPU) cannot be sent using
  Arrow IPC without having to copy the data back to the host device or copying
  the Flatbuffers metadata bytes into device memory.

  * By the same token, receiving IPC messages into device memory would require
    performing a copy of the Flatbuffers metadata back to the host CPU device. This
    is due to the fact that the IPC stream interleaves data and metadata across a
    single stream.

This protocol attempts to solve these use cases in an efficient manner.

Goals
-----

* Define a generic protocol for passing Arrow IPC data, not tied to any particular
  transport, that also allows for utilizing non-CPU device memory, shared memory, and
  newer "high performance" transports such as `UCX`_ or `libfabric`_.

  * This allows for the data in the body to be kept on non-CPU devices (like GPUs)
    without expensive device-to-host copies.

* Allow for using :ref:`Flight RPC <flight-rpc>` purely for control flow by separating
  the stream of IPC metadata from IPC body bytes

Definitions
-----------

IPC Metadata
    The Flatbuffers message bytes that encompass the header of an Arrow IPC message

Tag
    A little-endian ``uint64`` value used for flow control and used in determining
    how to interpret the body of a message. Specific bits can be masked to allow
    identifying messages by only a portion of the tag, leaving the rest of the bits
    to be used for control flow or other message metadata. Some transports, such as
    UCX, have built-in support for such tag values and will provide them in CPU
    memory regardless of whether or not the body of the message may reside on a
    non-CPU device.

Sequence Number
    A little-endian, 4-byte unsigned integer starting at 0 for a stream, indicating
    the sequence order of messages. It is also used to identify specific messages to
    tie the IPC metadata header to its corresponding body since the metadata and body
    can be sent across separate pipes/streams/transports.

    If a sequence number reaches ``UINT32_MAX``, it should be allowed to roll over as
    it is unlikely there would be enough unprocessed messages waiting to be processed
    that would cause an overlap of sequence numbers.

    The sequence number serves two purposes: To identify corresponding metadata and
    tagged body data messages and to ensure we do not rely on messages having to arrive
    in order. A client should use the sequence number to correctly order messages as
    they arrive for processing.

The Protocol
============

A reference example implementation utilizing `libcudf`_ and `UCX`_ can be found in the
`arrow-experiments repo <https://github.com/apache/arrow-experiments/tree/main/dissociated-ipc>`_.

Requirements
------------

A transport implementing this protocol **MUST** provide two pieces of functionality:

* Message sending

  * Delimited messages (like gRPC) as opposed to non-delimited streams (like plain TCP
    without further framing).

  * Alternatively, a framing mechanism like the :ref:`encapsulated message format <ipc-message-format>`
    for the IPC protocol can be used while leaving out the body bytes.

* Tagged message sending

  * Sending a message that has an attached little-endian, unsigned 64-bit integral tag
    for control flow. A tag like this allows control flow to operate on a message whose body
    is on a non-CPU device without requiring the message itself to get copied off of the device.

URI Specification
-----------------

When providing a URI to a consumer to contact for use with this protocol (such as via
the :ref:`Location URI for Flight <flight-location-uris>`), the URI should specify a scheme
like *ucx:* or *fabric:*, that is easily identifiable. In addition, the URI should
encode the following URI query parameters:

.. note::
    As this protocol matures, this document will get updated with commonly recognized
    transport schemes that get used with it.

* ``want_data`` - **REQUIRED** - uint64 integer value

  * This value should be used to tag an initial message to the server to initiate a
    data transfer. The body of the initiating message should be an opaque binary identifier
    of the data stream being requested (like the ``Ticket`` in the Flight RPC protocol)

* ``free_data`` - **OPTIONAL** - uint64 integer value

  * If the server might send messages using offsets / addresses for remote memory accessing
    or shared memory locations, the URI should include this parameter. This value is used to
    tag messages sent from the client to the data server, containing specific offsets / addresses
    which were provided that are no longer required by the client (i.e. any operations that
    directly reference those memory locations, such as copying the remote data into local memory,
    have been completed).

* ``remote_handle`` - **OPTIONAL** - base64-encoded string

  * When working with shared memory or remote memory, this value indicates any required
    handle or identifier that is necessary for accessing the memory.

    * Using UCX, this would be an *rkey* value

    * With CUDA IPC, this would be the value of the base GPU pointer or memory handle,
      and subsequent addresses would be offsets from this base pointer.

Handling of Backpressure
------------------------

*Currently* this proposal does not specify any way to manage the backpressure of
messages to throttle for memory and bandwidth reasons. For now, this will be
**transport-defined** rather than lock into something sub-optimal.

As usage among different transports and libraries grows, common patterns will emerge
that will allow for a generic, but efficient, way to handle backpressure across
different use cases.

.. note::
  While the protocol itself is transport agnostic, the current usage and examples
  only have been tested using UCX and libfabric transports so far, but that's all.


Protocol Description
====================

There are two possibilities that can occur:

1. The streams of metadata and body data are sent across separate connections

.. mermaid:: ./DissociatedIPC/SequenceDiagramSeparate.mmd


2. The streams of metadata and body data are sent simultaneously across the
   same connection

.. mermaid:: ./DissociatedIPC/SequenceDiagramSame.mmd

Server Sequence
---------------

There can be either a single server handling both the IPC Metadata stream and the
Body data streams, or separate servers for handling the IPC Metadata and the body
data. This allows for streaming of data across either a single transport pipe or
two pipes if desired.

Metadata Stream Sequence
''''''''''''''''''''''''

The standing state of the server is waiting for a **tagged** message with a specific
``<want_data>`` tag value to initiate a transfer. This ``<want_data>`` value is defined
by the server and propagated to any clients via the URI they are provided. This protocol
does not prescribe any particular value so that it will not interfere with any other
existing protocols that rely on tag values. The body of that message will contain an
opaque, binary identifier to indicate a particular dataset / data stream to send.

.. note::

  For instance, the **ticket** that was passed with a *FlightInfo* message would be
  the body of this message. Because it is opaque, it can be anything the server wants
  to use. The URI and identifier do not need to be given to the client via Flight RPC,
  but could come across from any transport or protocol desired.

Upon receiving a ``<want_data>`` request, the server *should* respond by sending a stream
of messages consisting of the following:

.. mermaid::

  block-beta
  columns 8

  block:P["\n\n\n\nPrefix"]:5
    T["Message type\nByte 0"]
    S["Sequence number\nBytes 1-4"]
  end
  H["Flatbuffer bytes\nRest of the message"]:3

* A 5-byte prefix

  - The first byte of the message indicates the type of message, currently there are only
    two allowed message types (more types may get added in the future):

    0) End of Stream
    1) Flatbuffers IPC Metadata Message

  - the next 4-bytes are a little-endian, unsigned 32-bit integer indicating the sequence number of
    the message. The first message in the stream (**MUST** always be a schema message) **MUST**
    have a sequence number of ``0``. Each subsequent message **MUST** increment the number by
    ``1``.

* The full Flatbuffers bytes of an Arrow IPC header

As defined in the Arrow IPC format, each metadata message can represent a chunk of data or
dictionaries for use by the stream of data.

After sending the last metadata message, the server **MUST** indicate the end of the stream
by sending a message consisting of **exactly** 5 bytes:

* The first byte is ``0``, indicating an **End of Stream** message
* The last 4 bytes are the sequence number (4-byte, unsigned integer in little-endian byte order)

Data Stream Sequence
''''''''''''''''''''

If a single server is handling both the data and metadata streams, then the data messages
**should** begin being sent to the client in parallel with the metadata messages. Otherwise,
as with the metadata sequence, the standing state of the server is to wait for a **tagged**
message with the ``<want_data>`` tag value, whose body indicates the dataset / data stream
to send to the client.

For each IPC message in the stream of data, a **tagged** message **MUST** be sent on the data
stream if that message has a body (i.e. a Record Batch or Dictionary message). The
:term:`tag <Tag>` for each message should be structured as follows:

.. mermaid::

  block-beta
  columns 8

  S["Sequence number\nBytes 0-3"]:4
  U["Unused (Reserved)\nBytes 4-6"]:3
  T["Message type\nByte 7"]:1

* The *least significant* 4-bytes (bits 0 - 31) of the tag should be the unsigned 32-bit, little-endian sequence
  number of the message.
* The *most significant* byte (bits 56 - 63) of the tag indicates the message body **type** as an 8-bit
  unsigned integer. Currently only two message types are specified, but more can be added as
  needed to expand the protocol:

  0) The body contains the raw body buffer bytes as a packed buffer (i.e. the standard IPC
     format body bytes)
  1) The body contains a series of unsigned, little-endian 64-bit integer pairs to represent
     either shared or remote memory, schematically structured as

     * The first two integers (e.g. the first 16 bytes) represent the *total* size (in bytes)
       of all buffers and the number of buffers in this message (and thus the number of following
       pairs of ``uint64``)

     * Each subsequent pair of ``uint64`` values are an address / offset followed the length of
       that particular buffer.

* All unspecified bits (bits 32 - 55) of the tag are *reserved* for future use by potential updates
  to this protocol. For now they **MUST** be 0.

.. note::

  Any shared/remote memory addresses that are sent across **MUST** be kept alive by the server
  until a corresponding tagged ``<free_data>`` message is received. If the client disconnects
  before sending any ``<free_data>`` messages, it can be assumed to be safe to clean up the memory
  if desired by the server.

After sending the last tagged IPC body message, the server should maintain the connection and wait
for tagged ``<free_data>`` messages. The structure of these ``<free_data>`` messages is simple:
one or more unsigned, little-endian 64-bit integers which indicate the addresses/offsets that can
be freed.

Once there are no more outstanding addresses to be freed, the work for this stream is complete.

Client Sequence
---------------

A client for this protocol needs to concurrently handle both the data and metadata streams of
messages which may either both come from the same server or different servers. Below is a flowchart
showing how a client might handle the metadata and data streams:

.. mermaid:: ./DissociatedIPC/ClientFlowchart.mmd

#. First the client sends a tagged message using the ``<want_data>`` value it was provided in the
   URI as the tag, and the opaque ID as the body.

   * If the metadata and data servers are separate, then a ``<want_data>`` message needs to be sent
     separately to each.
   * In either scenario, the metadata and data streams can be processed concurrently and/or asynchronously
     depending on the nature of the transports.

#. For each **untagged** message the client receives in the metadata stream:

   * The first byte of the message indicates whether it is an *End of Stream* message (value ``0``)
     or a metadata message (value ``1``).
   * The next 4 bytes are the sequence number of the message, an unsigned 32-bit integer in
     little-endian byte order.
   * If it is **not** an *End of Stream* message, the remaining bytes are the IPC Flatbuffer bytes which
     can be interpreted as normal.

     * If the message has a body (i.e. Record Batch or Dictionary message) then the client should retrieve
       a tagged message from the Data Stream using the same sequence number.

   * If it **is** an *End of Stream* message, then it is safe to close the metadata connection if there are
     no gaps in the sequence numbers received.

#. When a metadata message that requires a body is received, the tag mask of ``0x00000000FFFFFFFF`` **should**
   be used alongside the sequence number to match the message regardless of the higher bytes (e.g. we only
   care about matching the lower 4 bytes to the sequence number)

   * Once recieved, the Most Significant Byte's value determines how the client processes the body data:

     * If the most significant byte is 0: Then the body of the message is the raw IPC packed body buffers
       allowing it to easily be processed with the corresponding metadata header bytes.

     * If the most significant byte is 1: The body of the message will consist of a series of pairs of
       unsigned, 64-bit integers in little-endian byte order.

       * The first two integers represent *1)* the total size of all the body buffers together to allow
         for easy allocation if an intermediate buffer is needed and *2)* the number of buffers being sent (``nbuf``).

       * The rest of the message will be ``nbuf`` pairs of integers, one for each buffer. Each pair is
         *1)* the address / offset of the buffer and *2)* the length of that buffer. Memory can then be retrieved
         via shared or remote memory routines based on the underlying transport. These addresses / offsets **MUST**
         be retained so they can be sent back in ``<free_data>`` messages later, indicating to the server that
         the client no longer needs the shared memory.

#. Once an *End of Stream* message is received, the client should process any remaining un-processed
   IPC metadata messages.

#. After individual memory addresses / offsets are able to be freed by the remote server (in the case where
   it has sent these rather than the full body bytes), the client should send corresponding ``<free_data>`` messages
   to the server.

   * A single ``<free_data>`` message consists of an arbitrary number of unsigned 64-bit integer values, representing
     the addresses / offsets which can be freed. The reason for it being an *arbitrary number* is to allow a client
     to choose whether to send multiple messages to free multiple addresses or to coalesce multiple addresses into
     fewer messages to be freed (thus making the protocol less "chatty" if desired)

Continuing Development
======================

If you decide to try this protocol in your own environments and system, we'd love feedback and to learn about
your use case. As this is an **experimental** protocol currently, we need real-world usage in order to facilitate
improving it and finding the right generalizations to standardize on across transports.

Please chime in using the Arrow Developers Mailing list: https://arrow.apache.org/community/#mailing-lists

.. _Flatbuffers: http://github.com/google/flatbuffers
.. _UCX: https://openucx.org/
.. _libfabric: https://ofiwg.github.io/libfabric/
.. _libcudf: https://docs.rapids.ai/api
