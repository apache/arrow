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

.. _cpp_memory_management:

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
   builder.Resize(11);  // reserve enough space for 11 bytes
   builder.Append("hello ", 6);
   builder.Append("world", 5);

   auto maybe_buffer = builder.Finish();
   if (!maybe_buffer.ok()) {
      // ... handle buffer allocation error
   }
   std::shared_ptr<arrow::Buffer> buffer = *maybe_buffer;

If a Buffer is meant to contain values of a given fixed-width type (for
example the 32-bit offsets of a List array), it can be more convenient to
use the template :class:`arrow::TypedBufferBuilder` API::

   TypedBufferBuilder<int32_t> builder;
   builder.Reserve(2);  // reserve enough space for two int32_t values
   builder.Append(0x12345678);
   builder.Append(-0x765643210);

   auto maybe_buffer = builder.Finish();
   if (!maybe_buffer.ok()) {
      // ... handle buffer allocation error
   }
   std::shared_ptr<arrow::Buffer> buffer = *maybe_buffer;

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

The default memory pool depends on how Arrow C++ was compiled:

- if enabled at compile time, a `jemalloc <http://jemalloc.net/>`_ heap;
- otherwise, if enabled at compile time, a
  `mimalloc <https://github.com/microsoft/mimalloc>`_ heap;
- otherwise, the C library ``malloc`` heap.

Overriding the Default Memory Pool
----------------------------------

One can override the above selection algorithm by setting the
``ARROW_DEFAULT_MEMORY_POOL`` environment variable to one of the following
values: ``jemalloc``, ``mimalloc`` or ``system``.  This variable is inspected
once when Arrow C++ is loaded in memory (for example when the Arrow C++ DLL
is loaded).

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


Memory Profiling
================

On Linux, detailed profiles of memory allocations can be generated using perf record,
without any need to modify the binaries. These profiles can show the
traceback in addition to allocation parameters (like size).

.. TODO: This requires a debug build, right? Or maybe not if symbols in header file


.. note::
   If you profiling Arrow's tests on another platform, you can run the following docker container
   using archery:::

      archery docker run ubuntu-cpp bash
      /arrow/ci/scripts/cpp_build.sh /arrow /build
      cd build/cpp/debug
      ./arrow-array-test # Run a test
      apt-get update
      apt-get install -y linux-tools-generic
      alias perf=/usr/lib/linux-tools/<something>/perf


To track allocations, create probe points on each of the jemalloc methods used.
Collecting ``'$params'`` allows us to record the size of the allocations requested,
while collecting ``$retval`` allows us to record the address of recorded allocations,
so we can correlate them with the call to free/dealloc.

:: 

   perf probe -x libarrow.so je_arrow_mallocx '$params' 
   perf probe -x libarrow.so je_arrow_mallocx%return '$retval' 
   perf probe -x libarrow.so je_arrow_rallocx '$params' 
   perf probe -x libarrow.so je_arrow_rallocx%return '$retval' 
   perf probe -x libarrow.so je_arrow_dallocx '$params' 

Then you can record calls with associated tracebacks using ``perf record``. In this 
example, we are running the StructArray unit tests in Arrow::
   
   perf record -g --call-graph dwarf \
    -e probe_libarrow:je_arrow_mallocx \
    -e probe_libarrow:je_arrow_mallocx__return \
    -e probe_libarrow:je_arrow_rallocx \
    -e probe_libarrow:je_arrow_rallocx__return \
    -e probe_libarrow:je_arrow_dallocx \
    ./arrow-array-test --gtest_filter=StructArray*

.. TODO: What are the equivalent probe calls for mimalloc and system allocator?


.. code-block:: python

   import sys
   import re
   import json

   # Example non-traceback line
   # arrow-array-tes 14344 [003]  7501.073802: probe_libarrow:je_arrow_mallocx: (7fbcd20bb640) size=0x80 flags=6

   current = {}
   current_traceback = ''

   def new_row():
       global current_traceback
       current['traceback'] = current_traceback
       print(json.dumps(current))
       current_traceback = ''

   for line in sys.stdin:
       if line == '\n':
           continue
       elif line[0] == '\t':
           # traceback line
           current_traceback += line.strip("\t")
       else:
           line = line.rstrip('\n')
           if not len(current) == 0:
               new_row()
           parts = re.sub(' +', ' ', line).split(' ')

           parts.reverse()
           parts.pop() # file
           parts.pop() # "14344"
           parts.pop() # "[003]"

           current['time'] = float(parts.pop().rstrip(":"))
           current['event'] = parts.pop().rstrip(":")

           parts.pop() # (7fbcd20bddf0)
           if parts[-1] == "<-":
               parts.pop()
               parts.pop()

           params = {}

           for pair in parts:
               key, value = pair.split("=")
               params[key] = value

           current['params'] = params


Running the above script gives us JSON lines file with all the events parsed::

   > perf script | python3 /arrow/process_perf_events.py > processed_events.jsonl
   > head head processed_events.jsonl | cut -c -120
   {"time": 14814.954378, "event": "probe_libarrow:je_arrow_mallocx", "params": {"flags": "6", "size": "0x80"}, "traceback"
   {"time": 14814.95443, "event": "probe_libarrow:je_arrow_mallocx__return", "params": {"arg1": "0x7f4a97e09000"}, "traceba
   {"time": 14814.95448, "event": "probe_libarrow:je_arrow_mallocx", "params": {"flags": "6", "size": "0x40"}, "traceback":
   {"time": 14814.954486, "event": "probe_libarrow:je_arrow_mallocx__return", "params": {"arg1": "0x7f4a97e0a000"}, "traceb
   {"time": 14814.954502, "event": "probe_libarrow:je_arrow_rallocx", "params": {"flags": "6", "size": "0x40", "ptr": "0x7f
   {"time": 14814.954507, "event": "probe_libarrow:je_arrow_rallocx__return", "params": {"arg1": "0x7f4a97e0a040"}, "traceb
   {"time": 14814.954796, "event": "probe_libarrow:je_arrow_mallocx", "params": {"flags": "6", "size": "0x40"}, "traceback"
   {"time": 14814.954805, "event": "probe_libarrow:je_arrow_mallocx__return", "params": {"arg1": "0x7f4a97e0a080"}, "traceb
   {"time": 14814.954817, "event": "probe_libarrow:je_arrow_mallocx", "params": {"flags": "6", "size": "0x40"}, "traceback"
   {"time": 14814.95482, "event": "probe_libarrow:je_arrow_mallocx__return", "params": {"arg1": "0x7f4a97e0a0c0"}, "traceba


From there one can answer a number of questions. For example, the following script will
find which allocations were never freed, and print the associated tracebacks along with
the count of dangling allocations:

.. code-block:: python

   '''Find tracebacks of allocations with no corresponding free'''
   import sys
   import json
   from collections import defaultdict

   allocated = dict()

   for line in sys.stdin:
       line = line.rstrip('\n')
       data = json.loads(line)

       if data['event'] == "probe_libarrow:je_arrow_mallocx__return":
           address = data['params']['arg1']
           allocated[address] = data['traceback']
       if data['event'] == "probe_libarrow:je_arrow_rallocx__return":
           address = data['params']['arg1']
           allocated[address] = data['traceback']
       elif data['event'] == "probe_libarrow:je_arrow_dallocx":
           address = data['params']['ptr']
           del allocated[address]

   traceback_counts = defaultdict(int)

   for traceback in allocated.values():
       traceback_counts[traceback] += 1

   for traceback, count in sorted(traceback_counts.items(), key=lambda x: -x[1]):
       print("Num of dangling allocations:", count)
       print(traceback)


::

   > cat processed_events.jsonl | python3 /arrow/count_tracebacks.py
   Num of dangling allocations: 1
       7f4a9b4f7e3b arrow::(anonymous namespace)::JemallocAllocator::AllocateAligned+0x63 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fac3c arrow::BaseMemoryPoolImpl<arrow::(anonymous namespace)::JemallocAllocator>::Allocate+0x8e (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fc75a arrow::PoolBuffer::Reserve+0x16e (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fc99a arrow::PoolBuffer::Resize+0x190 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4f988a arrow::(anonymous namespace)::ResizePoolBuffer<std::unique_ptr<arrow::ResizableBuffer, std::default_delete<arrow::ResizableBuffer> >, std::unique_ptr<arrow::PoolBuffer, std::default_delete<arrow::PoolBuffer> > >+0x47 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4f9229 arrow::AllocateResizableBuffer+0x51 (/build/cpp/debug/libarrow.so.700.0.0)
       564fb42a178c arrow::BufferBuilder::Resize+0x60 (/build/cpp/debug/arrow-array-test)
       564fb4406d81 arrow::TypedBufferBuilder<int, void>::Resize+0x4f (/build/cpp/debug/arrow-array-test)
       564fb43fa751 arrow::NumericBuilder<arrow::Int32Type>::Resize+0xe7 (/build/cpp/debug/arrow-array-test)
       564fb42a26e0 arrow::ArrayBuilder::Reserve+0xaa (/build/cpp/debug/arrow-array-test)
       564fb42afc5a arrow::NumericBuilder<arrow::Int32Type>::Append+0x3e (/build/cpp/debug/arrow-array-test)
       7f4a9ca3c6c0 arrow::ipc::internal::json::(anonymous namespace)::IntegerConverter<arrow::Int32Type, arrow::NumericBuilder<arrow::Int32Type> >::AppendValue+0x10c (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca3c791 arrow::ipc::internal::json::(anonymous namespace)::ConcreteConverter<arrow::ipc::internal::json::(anonymous namespace)::IntegerConverter<arrow::Int32Type, arrow::NumericBuilder<arrow::Int32Type> > >::AppendValues+0xb1 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca0f1be arrow::ipc::internal::json::ArrayFromJSON+0x1e9 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9e533858 arrow::ArrayFromJSON+0x64 (/build/cpp/debug/libarrow_testing.so.700.0.0)
       564fb47b53f3 arrow::StructArray_FlattenOfSlice_Test::TestBody+0x79 (/build/cpp/debug/arrow-array-test)
       7f4a99d24633 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::Test, void>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1c32a testing::internal::HandleExceptionsInMethodIfSupported<testing::Test, void>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf05eb testing::Test::Run+0xf1 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf102d testing::TestInfo::Run+0x13f (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf1947 testing::TestSuite::Run+0x14b (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d013f5 testing::internal::UnitTestImpl::RunAllTests+0x433 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d25b61 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1d568 testing::internal::HandleExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cffb0c testing::UnitTest::Run+0xcc (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d6b299 RUN_ALL_TESTS+0x14 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a99d6b21b main+0x42 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a998820b2 __libc_start_main+0xf2 (/usr/lib/x86_64-linux-gnu/libc-2.31.so)
       564fb424850d _start+0x2d (/build/cpp/debug/arrow-array-test)

   Num of dangling allocations: 1
       7f4a9b4f7e3b arrow::(anonymous namespace)::JemallocAllocator::AllocateAligned+0x63 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fac3c arrow::BaseMemoryPoolImpl<arrow::(anonymous namespace)::JemallocAllocator>::Allocate+0x8e (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fc75a arrow::PoolBuffer::Reserve+0x16e (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fc99a arrow::PoolBuffer::Resize+0x190 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4f988a arrow::(anonymous namespace)::ResizePoolBuffer<std::unique_ptr<arrow::ResizableBuffer, std::default_delete<arrow::ResizableBuffer> >, std::unique_ptr<arrow::PoolBuffer, std::default_delete<arrow::PoolBuffer> > >+0x47 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4f9229 arrow::AllocateResizableBuffer+0x51 (/build/cpp/debug/libarrow.so.700.0.0)
       564fb42a178c arrow::BufferBuilder::Resize+0x60 (/build/cpp/debug/arrow-array-test)
       564fb4402803 arrow::TypedBufferBuilder<long, void>::Resize+0x4f (/build/cpp/debug/arrow-array-test)
       564fb43f6a3f arrow::NumericBuilder<arrow::Int64Type>::Resize+0xe7 (/build/cpp/debug/arrow-array-test)
       564fb42a26e0 arrow::ArrayBuilder::Reserve+0xaa (/build/cpp/debug/arrow-array-test)
       564fb42b5141 arrow::NumericBuilder<arrow::Int64Type>::Append+0x3f (/build/cpp/debug/arrow-array-test)
       7f4a9ca3c3b7 arrow::ipc::internal::json::(anonymous namespace)::IntegerConverter<arrow::Int64Type, arrow::NumericBuilder<arrow::Int64Type> >::AppendValue+0x10d (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca3c489 arrow::ipc::internal::json::(anonymous namespace)::ConcreteConverter<arrow::ipc::internal::json::(anonymous namespace)::IntegerConverter<arrow::Int64Type, arrow::NumericBuilder<arrow::Int64Type> > >::AppendValues+0xb1 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca0f1be arrow::ipc::internal::json::ArrayFromJSON+0x1e9 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9e533858 arrow::ArrayFromJSON+0x64 (/build/cpp/debug/libarrow_testing.so.700.0.0)
       564fb47afdf7 arrow::StructArray_FromFields_Test::TestBody+0x985 (/build/cpp/debug/arrow-array-test)
       7f4a99d24633 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::Test, void>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1c32a testing::internal::HandleExceptionsInMethodIfSupported<testing::Test, void>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf05eb testing::Test::Run+0xf1 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf102d testing::TestInfo::Run+0x13f (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf1947 testing::TestSuite::Run+0x14b (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d013f5 testing::internal::UnitTestImpl::RunAllTests+0x433 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d25b61 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1d568 testing::internal::HandleExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cffb0c testing::UnitTest::Run+0xcc (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d6b299 RUN_ALL_TESTS+0x14 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a99d6b21b main+0x42 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a998820b2 __libc_start_main+0xf2 (/usr/lib/x86_64-linux-gnu/libc-2.31.so)
       564fb424850d _start+0x2d (/build/cpp/debug/arrow-array-test)

   Num of dangling allocations: 1
       7f4a9b4f7fd2 arrow::(anonymous namespace)::JemallocAllocator::ReallocateAligned+0x13b (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fae4f arrow::BaseMemoryPoolImpl<arrow::(anonymous namespace)::JemallocAllocator>::Reallocate+0x93 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9b4fc8f7 arrow::PoolBuffer::Resize+0xed (/build/cpp/debug/libarrow.so.700.0.0)
       564fb42a1859 arrow::BufferBuilder::Resize+0x12d (/build/cpp/debug/arrow-array-test)
       564fb42a1bbe arrow::BufferBuilder::Finish+0x48 (/build/cpp/debug/arrow-array-test)
       564fb42a1e3a arrow::BufferBuilder::Finish+0x50 (/build/cpp/debug/arrow-array-test)
       564fb42a1f90 arrow::BufferBuilder::FinishWithLength+0x4e (/build/cpp/debug/arrow-array-test)
       564fb4406fa7 arrow::TypedBufferBuilder<int, void>::FinishWithLength+0x4f (/build/cpp/debug/arrow-array-test)
       564fb43face7 arrow::NumericBuilder<arrow::Int32Type>::FinishInternal+0x107 (/build/cpp/debug/arrow-array-test)
       7f4a9b2a15ae arrow::ArrayBuilder::Finish+0x5a (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca09d41 arrow::ipc::internal::json::(anonymous namespace)::Converter::Finish+0x123 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9ca0f26e arrow::ipc::internal::json::ArrayFromJSON+0x299 (/build/cpp/debug/libarrow.so.700.0.0)
       7f4a9e533858 arrow::ArrayFromJSON+0x64 (/build/cpp/debug/libarrow_testing.so.700.0.0)
       564fb47b53f3 arrow::StructArray_FlattenOfSlice_Test::TestBody+0x79 (/build/cpp/debug/arrow-array-test)
       7f4a99d24633 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::Test, void>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1c32a testing::internal::HandleExceptionsInMethodIfSupported<testing::Test, void>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf05eb testing::Test::Run+0xf1 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf102d testing::TestInfo::Run+0x13f (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cf1947 testing::TestSuite::Run+0x14b (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d013f5 testing::internal::UnitTestImpl::RunAllTests+0x433 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d25b61 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d1d568 testing::internal::HandleExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99cffb0c testing::UnitTest::Run+0xcc (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
       7f4a99d6b299 RUN_ALL_TESTS+0x14 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a99d6b21b main+0x42 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
       7f4a998820b2 __libc_start_main+0xf2 (/usr/lib/x86_64-linux-gnu/libc-2.31.so)
       564fb424850d _start+0x2d (/build/cpp/debug/arrow-array-test)


Some other resources with tracing:

https://www.maartenbreddels.com/perf/jupyter/python/tracing/gil/2021/01/14/Tracing-the-Python-GIL.html
https://jvns.ca/linux-tracing-zine.pdf
https://jvns.ca/perf-zine.pdf
https://www.brendangregg.com/blog/2015-06-28/linux-ftrace-uprobe.html