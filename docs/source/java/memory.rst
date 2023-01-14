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

The memory modules contain all the functionality that Arrow uses to allocate and deallocate memory. This document is divided in two parts:
The first part, *Memory Basics*, provides a high-level introduction. The following section, *Arrow Memory In-Depth*, fills in the details. 

.. contents::

Memory Basics
=============
This section will introduce you to the major concepts in Java’s memory management:

* `ArrowBuf`_
* `BufferAllocator`_
* Reference counting

It also provides some guidelines for working with memory in Arrow, and describes how to debug memory issues when they arise.

Getting Started
---------------

Arrow's memory management is built around the needs of the columnar format and using off-heap memory.
Arrow Java has its own independent implementation. It does not wrap the C++ implementation, although the framework is flexible enough
to be used with memory allocated in C++ that is used by Java code. 

Arrow provides multiple modules: the core interfaces, and implementations of the interfaces.
Users need the core interfaces, and exactly one of the implementations.

* ``memory-core``: Provides the interfaces used by the Arrow libraries and applications.
* ``memory-netty``: An implementation of the memory interfaces based on the `Netty`_ library.
* ``memory-unsafe``: An implementation of the memory interfaces based on the `sun.misc.Unsafe`_ library.


ArrowBuf
--------

ArrowBuf represents a single, contiguous region of `direct memory`_. It consists of an address and a length,
and provides low-level interfaces for working with the contents, similar to ByteBuffer.

Unlike (Direct)ByteBuffer, it has reference counting built in, as discussed later.

Why Arrow Uses Direct Memory
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* The JVM can optimize I/O operations when using direct memory/direct buffers; it will attempt to avoid copying buffer contents to/from an intermediate buffer. This can speed up IPC in Arrow.
* Since Arrow always uses direct memory, JNI modules can directly wrap native memory addresses instead of copying data. We use this in modules like the C Data Interface.
* Conversely, on the C++ side of the JNI boundary, we can directly access the memory in ArrowBuf without copying data.

BufferAllocator
---------------

The `BufferAllocator`_ is primarily an arena or nursery used for accounting of buffers (ArrowBuf instances). 
As the name suggests, it can allocate new buffers associated with itself, but it can also 
handle the accounting for buffers allocated elsewhere. For example, it handles the Java-side accounting for 
memory allocated in C++ and shared with Java using the C-Data Interface. In the code below it performs an allocation:

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
be set for a particular section of code. The child allocator can be closed when that section completes, 
at which point it checks that that section didn't leak any memory. 
Child allocators can also be named, which makes it easier to tell where an ArrowBuf came from during debugging.

Reference counting
------------------

Because direct memory is expensive to allocate and deallocate, allocators may share direct buffers. To managed shared buffers 
deterministically, we use manual reference counting instead of the garbage collector. 
This simply means that each buffer has a counter keeping track of the number of references to
the buffer, and the user is responsible for properly incrementing/decrementing the counter as the buffer is used.

In Arrow, each ArrowBuf has an associated `ReferenceManager`_ that tracks the reference count. You can retrieve
it with ArrowBuf.getReferenceManager(). The reference count is updated using `ReferenceManager.release`_ to decrement the count, 
and `ReferenceManager.retain`_ to increment it. 

Of course, this is tedious and error-prone, so instead of directly working with buffers, we typically use
higher-level APIs like ValueVector. Such classes generally implement Closeable/AutoCloseable and will automatically
decrement the reference count when closed.

Allocators implement AutoCloseable as well. In this case, closing the allocator will check that all buffers
obtained from the allocator are closed. If not, ``close()`` method will raise an exception; this helps track
memory leaks from unclosed buffers.

Reference counting needs to be handled carefully. To ensure that an
independent section of code has fully cleaned up all allocated buffers, use a new child allocator.

Development Guidelines
----------------------

Applications should generally:

* Use the BufferAllocator interface in APIs instead of RootAllocator.
* Create one RootAllocator at the start of the program.
* ``close()`` allocators after use (whether they are child allocators or the RootAllocator), either manually or preferably via a try-with-resources statement.


Debugging Memory Leaks/Allocation
---------------------------------

In ``DEBUG`` mode, the allocator and
supporting classes will record additional debug tracking information to
better track down memory leaks and issues. To enable DEBUG mode, either
enable Java assertions with ``-ea`` or pass the following system
property to the VM when starting
``-Darrow.memory.debug.allocator=true``. 

When DEBUG is enabled, a log will be kept of allocations. Configure SLF4J to see these logs (e.g. via Logback/Apache Log4j).
Consider the following example to see how it helps us with the tracking of allocators:

.. code-block:: Java

    import org.apache.arrow.memory.ArrowBuf;
    import org.apache.arrow.memory.BufferAllocator;
    import org.apache.arrow.memory.RootAllocator;

    try (BufferAllocator bufferAllocator = new RootAllocator(8 * 1024)) {
        ArrowBuf arrowBuf = bufferAllocator.buffer(4 * 1024);
        System.out.println(arrowBuf);
    }

Without the debug mode enabled, when we close the allocator, we get this:

.. code-block:: shell

    11:56:48.944 [main] INFO  o.apache.arrow.memory.BaseAllocator - Debug mode disabled.
    ArrowBuf[2], address:140508391276544, length:4096
    16:28:08.847 [main] ERROR o.apache.arrow.memory.BaseAllocator - Memory was leaked by query. Memory leaked: (4096)
    Allocator(ROOT) 0/4096/4096/8192 (res/actual/peak/limit)

Enabling the debug mode, we get more details:

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

Additionally, in debug mode, `ArrowBuf.print()`_ can be used to obtain a debug string.
This will include information about allocation operations on the buffer with stack traces, such as when/where the buffer was allocated.

.. code-block:: java

   import org.apache.arrow.memory.ArrowBuf;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;

   try (final BufferAllocator allocator = new RootAllocator()) {
     try (final ArrowBuf buf = allocator.buffer(1024)) {
       final StringBuilder sb = new StringBuilder();
       buf.print(sb, /*indent*/ 0);
       System.out.println(sb.toString());
     }
   }

.. code-block:: text

   ArrowBuf[2], address:140433199984656, length:1024
    event log for: ArrowBuf[2]
      675959093395667 create()
         at org.apache.arrow.memory.util.HistoricalLog$Event.<init>(HistoricalLog.java:175)
         at org.apache.arrow.memory.util.HistoricalLog.recordEvent(HistoricalLog.java:83)
         at org.apache.arrow.memory.ArrowBuf.<init>(ArrowBuf.java:96)
         at org.apache.arrow.memory.BufferLedger.newArrowBuf(BufferLedger.java:271)
         at org.apache.arrow.memory.BaseAllocator.bufferWithoutReservation(BaseAllocator.java:300)
         at org.apache.arrow.memory.BaseAllocator.buffer(BaseAllocator.java:276)
         at org.apache.arrow.memory.RootAllocator.buffer(RootAllocator.java:29)
         at org.apache.arrow.memory.BaseAllocator.buffer(BaseAllocator.java:240)
         at org.apache.arrow.memory.RootAllocator.buffer(RootAllocator.java:29)
         at REPL.$JShell$14.do_it$($JShell$14.java:10)
         at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(NativeMethodAccessorImpl.java:-2)
         at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
         at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
         at java.lang.reflect.Method.invoke(Method.java:566)
         at jdk.jshell.execution.DirectExecutionControl.invoke(DirectExecutionControl.java:209)
         at jdk.jshell.execution.RemoteExecutionControl.invoke(RemoteExecutionControl.java:116)
         at jdk.jshell.execution.DirectExecutionControl.invoke(DirectExecutionControl.java:119)
         at jdk.jshell.execution.ExecutionControlForwarder.processCommand(ExecutionControlForwarder.java:144)
         at jdk.jshell.execution.ExecutionControlForwarder.commandLoop(ExecutionControlForwarder.java:262)
         at jdk.jshell.execution.Util.forwardExecutionControl(Util.java:76)
         at jdk.jshell.execution.Util.forwardExecutionControlAndIO(Util.java:137)
         at jdk.jshell.execution.RemoteExecutionControl.main(RemoteExecutionControl.java:70)

The BufferAllocator also provides a ``BufferAllocator.toVerboseString()`` which can be used in
``DEBUG`` mode to get extensive stacktrace information and events associated with various Allocator behaviors.

Finally, enabling the ``TRACE`` logging level will automatically provide this stack trace when the allocator is closed:

.. code-block:: java

   // Assumes use of Logback; adjust for Log4j, etc. as appropriate
   import ch.qos.logback.classic.Level;
   import ch.qos.logback.classic.Logger;
   import org.apache.arrow.memory.ArrowBuf;
   import org.apache.arrow.memory.BufferAllocator;
   import org.apache.arrow.memory.RootAllocator;
   import org.slf4j.LoggerFactory;

   // Set log level to TRACE to get tracebacks
   ((Logger) LoggerFactory.getLogger("org.apache.arrow")).setLevel(Level.TRACE);
   try (final BufferAllocator allocator = new RootAllocator()) {
     // Leak buffer
     allocator.buffer(1024);
   }

.. code-block:: text

   |  Exception java.lang.IllegalStateException: Allocator[ROOT] closed with outstanding buffers allocated (1).
   Allocator(ROOT) 0/1024/1024/9223372036854775807 (res/actual/peak/limit)
     child allocators: 0
     ledgers: 1
       ledger[1] allocator: ROOT), isOwning: , size: , references: 1, life: 712040870231544..0, allocatorManager: [, life: ] holds 1 buffers.
           ArrowBuf[2], address:139926571810832, length:1024
        event log for: ArrowBuf[2]
          712040888650134 create()
                 at org.apache.arrow.memory.util.StackTrace.<init>(StackTrace.java:34)
                 at org.apache.arrow.memory.util.HistoricalLog$Event.<init>(HistoricalLog.java:175)
                 at org.apache.arrow.memory.util.HistoricalLog.recordEvent(HistoricalLog.java:83)
                 at org.apache.arrow.memory.ArrowBuf.<init>(ArrowBuf.java:96)
                 at org.apache.arrow.memory.BufferLedger.newArrowBuf(BufferLedger.java:271)
                 at org.apache.arrow.memory.BaseAllocator.bufferWithoutReservation(BaseAllocator.java:300)
                 at org.apache.arrow.memory.BaseAllocator.buffer(BaseAllocator.java:276)
                 at org.apache.arrow.memory.RootAllocator.buffer(RootAllocator.java:29)
                 at org.apache.arrow.memory.BaseAllocator.buffer(BaseAllocator.java:240)
                 at org.apache.arrow.memory.RootAllocator.buffer(RootAllocator.java:29)
                 at REPL.$JShell$18.do_it$($JShell$18.java:13)
                 at jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(NativeMethodAccessorImpl.java:-2)
                 at jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:62)
                 at jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43)
                 at java.lang.reflect.Method.invoke(Method.java:566)
                 at jdk.jshell.execution.DirectExecutionControl.invoke(DirectExecutionControl.java:209)
                 at jdk.jshell.execution.RemoteExecutionControl.invoke(RemoteExecutionControl.java:116)
                 at jdk.jshell.execution.DirectExecutionControl.invoke(DirectExecutionControl.java:119)
                 at jdk.jshell.execution.ExecutionControlForwarder.processCommand(ExecutionControlForwarder.java:144)
                 at jdk.jshell.execution.ExecutionControlForwarder.commandLoop(ExecutionControlForwarder.java:262)
                 at jdk.jshell.execution.Util.forwardExecutionControl(Util.java:76)
                 at jdk.jshell.execution.Util.forwardExecutionControlAndIO(Util.java:137)

     reservations: 0

   |        at BaseAllocator.close (BaseAllocator.java:405)
   |        at RootAllocator.close (RootAllocator.java:29)
   |        at (#8:1)

.. _`ArrowBuf`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html
.. _`ArrowBuf.print()`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ArrowBuf.html#print-java.lang.StringBuilder-int-org.apache.arrow.memory.BaseAllocator.Verbosity-
.. _`BufferAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/BufferAllocator.html
.. _`BufferLedger`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/BufferLedger.html
.. _`RootAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/RootAllocator.html
.. _`newChildAllocator`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/RootAllocator.html#newChildAllocator-java.lang.String-org.apache.arrow.memory.AllocationListener-long-long-
.. _`Netty`: https://netty.io/wiki/
.. _`sun.misc.unsafe`: https://web.archive.org/web/20210929024401/http://www.docjar.com/html/api/sun/misc/Unsafe.java.html
.. _`Direct Memory`: https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/nio/ByteBuffer.html
.. _`ReferenceManager`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html
.. _`ReferenceManager.release`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html#release--
.. _`ReferenceManager.retain`: https://arrow.apache.org/docs/java/reference/org/apache/arrow/memory/ReferenceManager.html#retain--

Arrow Memory In-Depth
=====================

Design Principles
-----------------
Arrow’s memory model is based on the following basic concepts:

-  Memory can be allocated up to some limit. That limit could be a real
   limit (OS/JVM) or a locally imposed limit.
-  Allocation operates in two phases: accounting then actual allocation.
   Allocation could fail at either point.
-  Allocation failure should be recoverable. In all cases, the Allocator
   infrastructure should expose memory allocation failures (OS or
   internal limit-based) as ``OutOfMemoryException``\ s.
-  Any allocator can reserve memory when created. This memory shall be
   held such that this allocator will always be able to allocate that
   amount of memory.
-  A particular application component should work to use a local
   allocator to understand local memory usage and better debug memory
   leaks.
-  The same physical memory can be shared by multiple allocators and the
   allocator must provide an accounting paradigm for this purpose.
   
Reserving Memory
----------------

Arrow provides two different ways to reserve memory:

-  BufferAllocator accounting reservations: When a new allocator (other
   than the ``RootAllocator``) is initialized, it can set aside memory
   that it will keep locally for its lifetime. This is memory that will
   never be released back to its parent allocator until the allocator is
   closed.
-  ``AllocationReservation`` via BufferAllocator.newReservation():
   Allows a short-term preallocation strategy so that a particular
   subsystem can ensure future memory is available to support a
   particular request.   
   
Reference Counting Details
--------------------------

Typically, the ReferenceManager implementation used is an instance of `BufferLedger`_. 
A BufferLedger is a ReferenceManager that also maintains the relationship between an ``AllocationManager``, 
a ``BufferAllocator`` and one or more individual ``ArrowBuf``\ s

All ArrowBufs (direct or sliced) related to a single BufferLedger/BufferAllocator combination 
share the same reference count and either all will be valid or all will be invalid. 
For simplicity of accounting, we treat that memory as being used by one
of the BufferAllocators associated with the memory. When that allocator
releases its claim on that memory, the memory ownership is then moved to
another BufferLedger belonging to the same AllocationManager.

Allocation Details
------------------

There are several Allocator types in Arrow Java:

-  ``BufferAllocator`` - The public interface application users should be leveraging
-  ``BaseAllocator`` - The base implementation of memory allocation, contains the meat of the Arrow allocator implementation
-  ``RootAllocator`` - The root allocator. Typically only one created for a JVM. It serves as the parent/ancestor for child allocators
-  ``ChildAllocator`` - A child allocator that derives from the root allocator

Many BufferAllocators can reference the same piece of physical memory at the same
time. It is the AllocationManager’s responsibility to ensure that in this situation, 
all memory is accurately accounted for from the Root’s perspective
and also to ensure that the memory is correctly released once all
BufferAllocators have stopped using that memory.

For simplicity of accounting, we treat that memory as being used by one
of the BufferAllocators associated with the memory. When that allocator
releases its claim on that memory, the memory ownership is then moved to
another BufferLedger belonging to the same AllocationManager. Note that
because a ArrowBuf.release() is what actually causes memory ownership
transfer to occur, we always proceed with ownership transfer (even if
that violates an allocator limit). It is the responsibility of the
application owning a particular allocator to frequently confirm whether
the allocator is over its memory limit (BufferAllocator.isOverLimit())
and if so, attempt to aggressively release memory to ameliorate the
situation.


Object Hierarchy
----------------

There are two main ways that someone can look at the object hierarchy
for Arrow’s memory management scheme. The first is a memory based
perspective as below:

Memory Perspective
~~~~~~~~~~~~~~~~~~

.. code-block:: none

   + AllocationManager
   |
   |-- UnsignedDirectLittleEndian (One per AllocationManager)
   |
   |-+ BufferLedger 1 ==> Allocator A (owning)
   | ` - ArrowBuf 1
   |-+ BufferLedger 2 ==> Allocator B (non-owning)
   | ` - ArrowBuf 2
   |-+ BufferLedger 3 ==> Allocator C (non-owning)
     | - ArrowBuf 3
     | - ArrowBuf 4
     ` - ArrowBuf 5

In this picture, a piece of memory is owned by an allocator manager. An
allocator manager is responsible for that piece of memory no matter
which allocator(s) it is working with. An allocator manager will have
relationships with a piece of raw memory (via its reference to
UnsignedDirectLittleEndian) as well as references to each
BufferAllocator it has a relationship to.

Allocator Perspective
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: none

   + RootAllocator
   |-+ ChildAllocator 1
   | | - ChildAllocator 1.1
   | ` ...
   |
   |-+ ChildAllocator 2
   |-+ ChildAllocator 3
   | |
   | |-+ BufferLedger 1 ==> AllocationManager 1 (owning) ==> UDLE
   | | `- ArrowBuf 1
   | `-+ BufferLedger 2 ==> AllocationManager 2 (non-owning)==> UDLE
   |   `- ArrowBuf 2
   |
   |-+ BufferLedger 3 ==> AllocationManager 1 (non-owning)==> UDLE
   | ` - ArrowBuf 3
   |-+ BufferLedger 4 ==> AllocationManager 2 (owning) ==> UDLE
     | - ArrowBuf 4
     | - ArrowBuf 5
     ` - ArrowBuf 6

In this picture, a RootAllocator owns three ChildAllocators. The first
ChildAllocator (ChildAllocator 1) owns a subsequent ChildAllocator.
ChildAllocator has two BufferLedgers/AllocationManager references.
Coincidentally, each of these AllocationManager’s is also associated
with the RootAllocator. In this case, one of the these
AllocationManagers is owned by ChildAllocator 3 (AllocationManager 1)
while the other AllocationManager (AllocationManager 2) is
owned/accounted for by the RootAllocator. Note that in this scenario,
ArrowBuf 1 is sharing the underlying memory as ArrowBuf 3. However the
subset of that memory (e.g. through slicing) might be different. Also
note that ArrowBuf 2 and ArrowBuf 4, 5 and 6 are also sharing the same
underlying memory. Also note that ArrowBuf 4, 5 and 6 all share the same
reference count and fate.
