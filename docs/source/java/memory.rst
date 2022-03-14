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

=================
Memory Management
=================

.. contents::

The memory modules contain all the functionality that Arrow uses to manage memory (allocation and deallocation).
This section will introduce you to the major concepts in Java’s memory management:

* `ArrowBuf`_
* `Reference counting`_
* `BufferAllocator`_

Getting Started
===============

Arrow's memory management is built around the needs of the columnar format and using off-heap memory.
Also, it is its own independent implementation, and does not wrap the C++ implementation.

Arrow offer a high level of abstraction providing several access APIs to read/write data into a direct memory.

Arrow provides multiple modules, but users only need two of them:

* ``Memory Core``: Provides the interfaces used by the Arrow libraries and applications.
* ``Memory Netty``: An implementation of the memory interfaces based on the `Netty`_ library.
* ``Memory Unsafe``: An implementation of the memory interfaces based on the `sun.misc.Unsafe`_ library.

ArrowBuf
========

ArrowBuf represents a single, contiguous allocation of direct memory. It consists of an address and a length,
and provides low-level interfaces for working with the contents, similar to ByteBuffer.

Unlike (Direct)ByteBuffer, it has reference counting built in (see the next section).

Reference counting
==================

Is a technique to help computer programs manage memory. Tracks the reference/pointers to an object, it increase
+1 or decrease -1 the reference counting between the objects.

If an object ValidityBuffer has a reference with object IntVector, then, IntVector should increase the
reference counting to 1 (0 + 1 = 1), then if at the same time, ValueBuffer has a reference with IntVector,
then, IntVector should increase the reference counting to 2 (1 + 1 = 2).

.. code-block::

    |__ A = Allocator
    |____ B = IntVector (reference count = 2 )
    |____________ ValidityBuffer
    |____________ ValueBuffer
    |____ C = VarcharVector (reference count = 2 )
    |____________ ValidityBuffer
    |____________ ValueBuffer

Base on best practices at some point you are going to close your allocator objects using ``close()`` method,
allocators check for reference counting and throw an exception if they are in use.

Reference Manager manages the reference counting for the underlying memory chunk.

Allocators
==========

One of the interfaces defined by memory-core is BufferAllocator. This interface collect all the definitions for deal
with byte buffer allocation.

The concrete implementation of the allocator is Root Allocator. Applications should generally
create one allocator at the start of the program..

Arrow provides a tree-based model for memory allocation. The RootAllocator is created first,
then all allocators are created as children of that allocator. The RootAllocator is responsible
for being the master bookkeeper for memory allocations.

As an example of child allocator consider `Flight Client`_ creation.

Memory Modules
==============

Applications should depend on memory-core and one of the two implementations,
else an exception will be raised at runtime.

Development Guidelines
======================

* Use the BufferAllocator interface in APIs instead of RootAllocator.
* Applications should generally create one allocator at the start of the program.
* Remember to close() allocators after use (whether they are child allocators or the RootAllocator), either manually or preferably via a try-with-resources statement.
* Allocators will check for outstanding memory allocations when closed, and throw an exception if there are allocated buffers, this helps detect memory leaks.
* Allocators have a debug mode, that makes it easier to figure out where a leak originated (Consider to add this parameter to your application: -Darrow.memory.debug.allocator=true)
* Arrow modules use logback to collect logs configure that properly to see your logs (create logback-test.xml file on resources folder).

.. _`BufferAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/BufferAllocator.html
.. _`ArrowBuf`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html
.. _`Reference Counting`: https://netty.io/wiki/reference-counted-objects.html#reference-counting-in-channelhandler
.. _`Netty`: https://netty.io/wiki/
.. _`sun.misc.unsafe`: https://web.archive.org/web/20210929024401/http://www.docjar.com/html/api/sun/misc/Unsafe.java.html
.. _`Flight Client`: https://github.com/apache/arrow/blob/a8eb73699b32ae36b2dd218e3eb969ec2cebd449/java/flight/flight-core/src/main/java/org/apache/arrow/flight/FlightClient.java#L96
