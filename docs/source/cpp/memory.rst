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

.. default-domain:: cpp
.. highlight:: cpp

=================
Memory Management
=================

.. seealso::
   :doc:`Memory management API reference <api/memory>`

Buffers
=======

To avoid passing around raw data pointers with varying and non-obvious
lifetime rules, Arrow provides a generic abstraction called :class:`arrow::Buffer`.
A Buffer encapsulates a pointer and data size, and generally also ties its
lifetime to that of an underlying provider (in other words, a Buffer should
*always* point to valid memory till its destruction).  Buffers are untyped:
they simply denote a physical memory area regardless of its intended meaning
or interpretation.

Buffers may be allocated by Arrow itself , or by third-party routines.
For example, it is possible to pass the data of a Python bytestring as a Arrow
buffer, keeping the Python object alive as necessary.

In addition, buffers come in various flavours: mutable or not, resizable or
not.  Generally, you will hold a mutable buffer when building up a piece
of data, then it will be frozen as an immutable container such as an
:doc:`array <arrays>`.

.. note::
   Some buffers may point to non-CPU memory, such as GPU-backed memory
   provided by a CUDA context.  If you're writing a GPU-aware application,
   you will need to be careful not to interpret a GPU memory pointer as
   a CPU-reachable pointer, or vice-versa.

Accessing Buffer Memory
-----------------------

Buffers provide fast access to the underlying memory using the
:func:`~arrow::Buffer::size` and :func:`~arrow::Buffer::data` accessors
(or :func:`~arrow::Buffer::mutable_data` for writable access to a mutable
buffer).

Slicing
-------

It is possible to make zero-copy slices of buffers, to obtain a buffer
referring to some contiguous subset of the underlying data.  This is done
by calling the :func:`arrow::SliceBuffer` and :func:`arrow::SliceMutableBuffer`
functions.

Allocating a Buffer
-------------------

You can allocate a buffer yourself by calling one of the
:func:`arrow::AllocateBuffer` or :func:`arrow::AllocateResizableBuffer`
overloads::

   arrow::Result<std::unique_ptr<Buffer>> maybe_buffer = arrow::AllocateBuffer(4096);
   if (!maybe_buffer.ok()) {
      // ... handle allocation error
   }

   std::shared_ptr<arrow::Buffer> buffer = *std::move(maybe_buffer);
   uint8_t* buffer_data = buffer->mutable_data();
   memcpy(buffer_data, "hello world", 11);

Allocating a buffer this way ensures it is 64-bytes aligned and padded
as recommended by the :doc:`Arrow memory specification <../format/Layout>`.

Building a Buffer
-----------------

You can also allocate *and* build a Buffer incrementally, using the
:class:`arrow::BufferBuilder` API::

   BufferBuilder builder;
   builder.Resize(11);
   builder.Append("hello ", 6);
   builder.Append("world", 5);

   std::shared_ptr<arrow::Buffer> buffer;
   if (!builder.Finish(&buffer).ok()) {
      // ... handle buffer allocation error
   }

Memory Pools
============

When allocating a Buffer using the Arrow C++ API, the buffer's underlying
memory is allocated by a :class:`arrow::MemoryPool` instance.  Usually this
will be the process-wide *default memory pool*, but many Arrow APIs allow
you to pass another MemoryPool instance for their internal allocations.

Memory pools are used for large long-lived data such as array buffers.
Other data, such as small C++ objects and temporary workspaces, usually
goes through the regular C++ allocators.

Default Memory Pool
-------------------

Depending on how Arrow was compiled, the default memory pool may use the
standard C ``malloc`` allocator, or a `jemalloc <http://jemalloc.net/>`_ heap.

STL Integration
---------------

If you wish to use a Arrow memory pool to allocate the data of STL containers,
you can do so using the :class:`arrow::stl::allocator` wrapper.

Conversely, you can also use a STL allocator to allocate Arrow memory,
using the :class:`arrow::stl::STLMemoryPool` class.  However, this may be less
performant, as STL allocators don't provide a resizing operation.

Devices
=======

Many Arrow applications only access host (CPU) memory.  However, in some cases
it is desirable to handle on-device memory (such as on-board memory on a GPU)
as well as host memory.

Arrow represents the CPU and other devices using the
:class:`arrow::Device` abstraction.  The associated class :class:`arrow::MemoryManager`
specifies how to allocate on a given device.  Each device has a default memory manager, but
additional instances may be constructed (for example, wrapping a custom
:class:`arrow::MemoryPool` the CPU).
:class:`arrow::MemoryManager` instances which specify how to allocate
memory on a given device (for example, using a particular
:class:`arrow::MemoryPool` on the CPU).

Device-Agnostic Programming
---------------------------

If you receive a Buffer from third-party code, you can query whether it is
CPU-readable by calling its :func:`~arrow::Buffer::is_cpu` method.

You can also view the Buffer on a given device, in a generic way, by calling
:func:`arrow::Buffer::View` or :func:`arrow::Buffer::ViewOrCopy`.  This will
be a no-operation if the source and destination devices are identical.
Otherwise, a device-dependent mechanism will attempt to construct a memory
address for the destination device that gives access to the buffer contents.
Actual device-to-device transfer may happen lazily, when reading the buffer
contents.

Similarly, if you want to do I/O on a buffer without assuming a CPU-readable
buffer, you can call :func:`arrow::Buffer::GetReader` and
:func:`arrow::Buffer::GetWriter`.

For example, to get an on-CPU view or copy of an arbitrary buffer, you can
simply do::

   std::shared_ptr<arrow::Buffer> arbitrary_buffer = ... ;
   std::shared_ptr<arrow::Buffer> cpu_buffer = arrow::Buffer::ViewOrCopy(
      arbitrary_buffer, arrow::default_cpu_memory_manager());
