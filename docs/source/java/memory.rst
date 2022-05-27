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

The memory modules contain all the functionality that Arrow uses to manage memory (allocation and deallocation).
This section will introduce you to the major concepts in Java’s memory management:

* `ArrowBuf`_
* `BufferAllocator`_
* Reference counting

.. contents::

Getting Started
===============

Arrow's memory management is built around the needs of the columnar format and using off-heap memory.
Also, it is its own independent implementation, and does not wrap the C++ implementation.

Arrow provides multiple modules: the core interfaces, and implementations of the interfaces.
Users need the core interfaces, and exactly one of the implementations.

* ``memory-core``: Provides the interfaces used by the Arrow libraries and applications.
* ``memory-netty``: An implementation of the memory interfaces based on the `Netty`_ library.
* ``memory-unsafe``: An implementation of the memory interfaces based on the `sun.misc.Unsafe`_ library.

ArrowBuf
========

ArrowBuf represents a single, contiguous region of `direct memory`_. It consists of an address and a length,
and provides low-level interfaces for working with the contents, similar to ByteBuffer.

Unlike (Direct)ByteBuffer, it has reference counting built in, as discussed later.

Why Arrow Uses Direct Memory
----------------------------

* The JVM can optimize I/O operations when using direct memory/direct buffers; it will attempt to avoid copying buffer contents to/from an intermediate buffer. This can speed up IPC in Arrow.
* Since Arrow always uses direct memory, JNI modules can directly wrap native memory addresses instead of copying data. We use this in modules like the C Data Interface.
* Conversely, on the C++ side of the JNI boundary, we can directly access the memory in ArrowBuf without copying data.

BufferAllocator
===============

The `BufferAllocator`_ interface deals with allocating ArrowBufs for the application.

.. code-block:: Java

    import org.apache.arrow.memory.ArrowBuf;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    try(BufferAllocator bufferAllocator = new RootAllocator(8 * 1024)){
        ArrowBuf arrowBuf = bufferAllocator.buffer(4 * 1024);
        System.out.println(arrowBuf);
        arrowBuf.close();
    }

.. code-block:: shell

    ArrowBuf[2], address:140363641651200, length:4096

The concrete implementation of the BufferAllocator interface is `RootAllocator`_. Applications should generally create
one RootAllocator at the start of the program, and use it through the BufferAllocator interface. Allocators implement
AutoCloseable and must be closed after the application is done with them; this will check that all outstanding memory
has been freed (see the next section).

Arrow provides a tree-based model for memory allocation. The RootAllocator is created first, then more allocators
are created as children of an existing allocator via `newChildAllocator`_. When creating a RootAllocator or a child
allocator, a memory limit is provided, and when allocating memory, the limit is checked. Furthermore, when allocating
memory from a child allocator, those allocations are also reflected in all parent allocators. Hence, the RootAllocator
effectively sets the program-wide memory limit, and serves as the master bookkeeper for all memory allocations.

Child allocators are not strictly required, but can help better organize code. For instance, a lower memory limit can
be set for a particular section of code. When the allocator is closed, it then checks that that section didn't leak any
memory. And child allocators can be named, which makes it easier to tell where an ArrowBuf came from during debugging.

Reference counting
==================

Direct memory is more expensive to allocate and deallocate. That's why allocators pool or cache direct buffers.

Because we want to pool/cache buffers and manage them deterministically, we use manual reference counting instead of
the garbage collector. This simply means that each buffer has a counter keeping track of the number of references to
the buffer, and the user is responsible for properly incrementing/decrementing the counter as the buffer is used.

In Arrow, each ArrowBuf has an associated `ReferenceManager`_ that tracks the reference count, which can be retrieved
with ArrowBuf.getReferenceManager(). The reference count can be updated with `ReferenceManager.release`_ and
`ReferenceManager.retain`_.

Of course, this is tedious and error-prone, so usually, instead of directly working with buffers, we should use
higher-level APIs like ValueVector. Such classes generally implement Closeable/AutoCloseable and will automatically
decrement the reference count when closed.

Allocators implement AutoCloseable as well. In this case, closing the allocator will check that all buffers
obtained from the allocator are closed. If not, ``close()`` method will raise an exception; this helps track
memory leaks from unclosed buffers.

As you see, reference counting needs to be handled carefully. To ensure that an
independent section of code has fully cleaned up all allocated buffers, use a new child allocator.

Development Guidelines
======================

Applications should generally:

* Use the BufferAllocator interface in APIs instead of RootAllocator.
* Create one RootAllocator at the start of the program.
* ``close()`` allocators after use (whether they are child allocators or the RootAllocator), either manually or preferably via a try-with-resources statement.

Debugging Memory Leaks/Allocation
=================================

Allocators have a debug mode that makes it easier to figure out where a leak is originated.
To enable it, enable assertions with ``-ea`` or set the system property, ``-Darrow.memory.debug.allocator=true``.
When enabled, a log will be kept of allocations.

Arrow logs some allocation information via SLF4J; configure it properly to see these logs (e.g. via Logback/Apache Log4j).

Consider the following example to see how debug enabled help us with the tracking of allocators:

.. code-block:: Java

    import org.apache.arrow.memory.ArrowBuf;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    try(BufferAllocator bufferAllocator = new RootAllocator(8 * 1024)){
        ArrowBuf arrowBuf = bufferAllocator.buffer(4 * 1024);
        System.out.println(arrowBuf);
    }

Logs without debug allocators enabled:

.. code-block:: shell

    11:56:48.944 [main] INFO  o.apache.arrow.memory.BaseAllocator - Debug mode disabled.
    ArrowBuf[2], address:140508391276544, length:4096
    16:28:08.847 [main] ERROR o.apache.arrow.memory.BaseAllocator - Memory was leaked by query. Memory leaked: (4096)
    Allocator(ROOT) 0/4096/4096/8192 (res/actual/peak/limit)

Logs with debug allocators enabled:

.. code-block:: shell

    11:56:48.944 [main] INFO  o.apache.arrow.memory.BaseAllocator - Debug mode enabled.
    ArrowBuf[2], address:140437894463488, length:4096
    Exception in thread "main" java.lang.IllegalStateException: Allocator[ROOT] closed with outstanding buffers allocated (1).
    Allocator(ROOT) 0/4096/4096/8192 (res/actual/peak/limit)
      child allocators: 0
      ledgers: 1
        ledger[1] allocator: ROOT), isOwning: , size: , references: 1, life: 261438177096661..0, allocatorManager: [, life: ] holds 1 buffers.
            ArrowBuf[2], address:140437894463488, length:4096
      reservations: 0

.. _`ArrowBuf`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html
.. _`BufferAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/BufferAllocator.html
.. _`RootAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/RootAllocator.html
.. _`newChildAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/RootAllocator.html#newChildAllocator-java.lang.String-org.apache.arrow.memory.AllocationListener-long-long-
.. _`Netty`: https://netty.io/wiki/
.. _`sun.misc.unsafe`: https://web.archive.org/web/20210929024401/http://www.docjar.com/html/api/sun/misc/Unsafe.java.html
.. _`Direct Memory`: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ByteBuffer.html
.. _`ReferenceManager`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html
.. _`ReferenceManager.release`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html#release--
.. _`ReferenceManager.retain`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html#retain--