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

* `BufferAllocator`_
* `ArrowBuf`_
* `Reference counting`_

Getting Started
===============

Arrow's memory management is built around the needs of the columnar format and using off-heap memory.
Also, it is its own independent implementation, and does not wrap the C++ implementation.

Arrow offers a high level of abstraction providing several access APIs to read/write data into a direct memory.

Arrow provides multiple modules: the core interfaces, and implementations of the interfaces.
Users need the core interfaces, and exactly one of the implementations.

* ``Memory Core``: Provides the interfaces used by the Arrow libraries and applications.
* ``Memory Netty``: An implementation of the memory interfaces based on the `Netty`_ library.
* ``Memory Unsafe``: An implementation of the memory interfaces based on the `sun.misc.Unsafe`_ library.

BufferAllocator
===============

The BufferAllocator interface deals with allocating ArrowBufs for the application.

The concrete implementation of the allocator is RootAllocator. Applications should generally create one RootAllocator at the
start of the program, and use it through the BufferAllocator interface. Allocators have a memory limit. The RootAllocator
sets the program-wide memory limit. The RootAllocator is responsible for being the master bookkeeper for memory allocations.

Arrow provides a tree-based model for memory allocation. The RootAllocator is created first, then all allocators
are created as children ``BufferAllocator.newChildAllocator`` of that allocator.

One of the uses of child allocators is to set a lower temporary limit for one section of the code. Also, child
allocators can be named; this makes it easier to tell where an ArrowBuf came from during debugging.

ArrowBuf
========

ArrowBuf represents a single, contiguous allocation of `Direct Memory`_. It consists of an address and a length,
and provides low-level interfaces for working with the contents, similar to ByteBuffer.

The objects created using ``Direct Memory`` take advantage of native executions and it is decided natively by the JVM. Arrow
offer efficient memory operations base on this Direct Memory implementation (`see section below for detailed reasons of use`).

Unlike (Direct)ByteBuffer, it has reference counting built in (`see the next section`).

Reference counting
==================

Direct memory involve more activities than allocate and deallocate because allocators (thru pool/cache)
allocate buffers (ArrowBuf).

Arrow uses manual reference counting to track whether a buffer is in use, or can be deallocated or returned
to the allocator's pool. This simply means that each buffer has a counter keeping track of the number of references to
this buffer, and end user is responsible for properly incrementing/decrementing the counter according the buffer is used.

In Arrow, each ArrowBuf has an associated ReferenceManager that tracks the reference count, which can be retrieved
with ArrowBuf.getReferenceManager(). The reference count can be updated with ``ReferenceManager.release`` and
``ReferenceManager.retain``.

Of course, this is tedious and error-prone, so usually, instead of directly working with buffers, we should use
higher-level APIs like ValueVector. Such classes generally implement Closeable/AutoCloseable and will automatically
decrement the reference count when closed method.

.. code-block::

    |__ A = Allocator
    |____ B = IntVector (reference count = 2 )
    |____________ ValidityBuffer
    |____________ ValueBuffer
    |____ C = VarcharVector (reference count = 2 )
    |____________ ValidityBuffer
    |____________ ValueBuffer

Allocators implement AutoCloseable as well. In this case, closing the allocator will check that all buffers
obtained from the allocator are closed. If not, ``close()`` method will raise an exception; this helps track
memory leaks from unclosed buffers.

As you see reference counting needs to be handled properly by us, if at some point you need to ensuring that an
independent section of code has `fully cleaned up all allocated buffers while still maintaining a global memory limit
through the RootAllocator`, well ``BufferAllocator.newChildAllocator`` is what you should use.

Reason To Use Direct Memory
===========================

* When `writing an ArrowBuf`_ we use the direct buffer (``nioBuffer()`` returns a DirectByteBuffer) and the JVM `will attempt to avoid copying the buffer's content to (or from) an intermediate buffer`_ so it makes I/O (and hence IPC) faster.
* We can `directly wrap a native memory address`_ instead of having to copy data for JNI (where in implementing the C Data Interface we can directly create `Java ArrowBufs that directly correspond to the C pointers`_).
* Conversely in JNI, we can directly use `Java ArrowBufs in C++`_ without having to copy data.

So basically #1 is more efficient I/O, and #2/#3 is better integration with JNI code.

Development Guidelines
======================

* Use the BufferAllocator interface in APIs instead of RootAllocator.
* Applications should generally create one RootAllocator at the start of the program.
* Remember to close() allocators after use (whether they are child allocators or the RootAllocator), either manually or preferably via a try-with-resources statement.

Debugging Memory Leaks/Allocation
=================================

Allocators have a debug mode, that makes it easier to figure out where a leak is originated (Consider to add this
parameter to your application: ``-Darrow.memory.debug.allocator=true``). This parameter enable to create an historical log
about the memory allocation.

Arrow modules use logback to collect logs, configure it properly to see your logs (create ``logback-test.xml`` file on
resources folder and your project could read that by conventions).

This is an example of historical log enabled:

.. code-block::

    15:49:32,755 |-INFO in ch.qos.logback.classic.LoggerContext[default] - Found resource [logback-test.xml] at [file:/Users/java/source/demo/target/classes/logback-test.xml]
    15:49:32,924 |-INFO in ch.qos.logback.classic.joran.action.LoggerAction - Setting level of logger [org.apache.arrow] to DEBUG
    11:56:48.944 [main] INFO  o.apache.arrow.memory.BaseAllocator - Debug mode enabled.
    Exception in thread "main" java.lang.IllegalStateException: Allocator[ROOT] closed with outstanding child allocators.
    Allocator(ROOT) 0/96/96/2147483647 (res/actual/peak/limit)
      child allocators: 1
        Allocator(child-isolated) 0/32/32/536870911 (res/actual/peak/limit)
          child allocators: 0
          ledgers: 1
            ledger[3] allocator: child-isolated), isOwning: , size: , references: 2, life: 216090048094500..0, allocatorManager: [, life: ] holds 3 buffers.
                ArrowBuf[10], address:140663354032216, length:8
                ArrowBuf[8], address:140663354032192, length:32
                ArrowBuf[9], address:140663354032192, length:24
          reservations: 0
      ledgers: 2
        ledger[2] allocator: ROOT), isOwning: , size: , references: 2, life: 216090045874483..0, allocatorManager: [, life: ] holds 3 buffers.
            ArrowBuf[7], address:140663354032184, length:8
            ArrowBuf[5], address:140663354032160, length:32
            ArrowBuf[6], address:140663354032160, length:24
        ledger[1] allocator: ROOT), isOwning: , size: , references: 2, life: 216090021161552..0, allocatorManager: [, life: ] holds 3 buffers.
            ArrowBuf[3], address:140663354032128, length:24
            ArrowBuf[4], address:140663354032152, length:8
            ArrowBuf[2], address:140663354032128, length:32
      reservations: 0

.. _`BufferAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/BufferAllocator.html
.. _`ArrowBuf`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html
.. _`Reference Counting`: https://github.com/apache/arrow/blob/2092e18752a9c0494799493b12eb1830052217a2/java/memory/memory-core/src/main/java/org/apache/arrow/memory/ReferenceManager.java#L30
.. _`Netty`: https://netty.io/wiki/
.. _`sun.misc.unsafe`: https://web.archive.org/web/20210929024401/http://www.docjar.com/html/api/sun/misc/Unsafe.java.html
.. _`Flight Client`: https://github.com/apache/arrow/blob/a8eb73699b32ae36b2dd218e3eb969ec2cebd449/java/flight/flight-core/src/main/java/org/apache/arrow/flight/FlightClient.java#L96
.. _`Direct Memory`: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ByteBuffer.html
.. _`writing an ArrowBuf`: https://github.com/apache/arrow/blob/3bf061783f4e1ab447d2eb0f487c0c4fce6d5b15/java/vector/src/main/java/org/apache/arrow/vector/ipc/WriteChannel.java#L133-L135
.. _`will attempt to avoid copying the buffer's content to (or from) an intermediate buffer`: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ByteBuffer.html
.. _`directly wrap a native memory address`: https://github.com/apache/arrow/blob/3bf061783f4e1ab447d2eb0f487c0c4fce6d5b15/java/c/src/main/java/org/apache/arrow/c/ArrowArray.java#L102-L104
.. _`Java ArrowBufs that directly correspond to the C pointers`: https://github.com/apache/arrow/blob/3bf061783f4e1ab447d2eb0f487c0c4fce6d5b15/java/c/src/main/java/org/apache/arrow/c/ArrayImporter.java#L130-L151
.. _`Java ArrowBufs in C++`: https://github.com/apache/arrow/blob/3bf061783f4e1ab447d2eb0f487c0c4fce6d5b15/cpp/src/gandiva/jni/jni_common.cc#L699-L723
