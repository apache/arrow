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

.. _cpp_memory_pool:

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
:envvar:`ARROW_DEFAULT_MEMORY_POOL` environment variable.

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

On Linux, detailed profiles of memory allocations can be generated using 
``perf record``, without any need to modify the binaries. These profiles can
show the traceback in addition to allocation size. This does require debug
symbols, from either a debug build or a release with debug symbols build.

.. note::
   If you are profiling Arrow's tests on another platform, you can run the
   following Docker container using Archery to access a Linux environment:

   .. code-block:: shell

      archery docker run ubuntu-cpp bash
      # Inside the Docker container...
      /arrow/ci/scripts/cpp_build.sh /arrow /build
      cd build/cpp/debug
      ./arrow-array-test # Run a test
      apt-get update
      apt-get install -y linux-tools-generic
      alias perf=/usr/lib/linux-tools/<version-path>/perf


To track allocations, create probe points on each of the allocator methods used.
Collecting ``$params`` allows us to record the size of the allocations
requested, while collecting ``$retval`` allows us to record the address of
recorded allocations, so we can correlate them with the call to free/de-allocate.

.. tabs::

   .. tab:: jemalloc
      
      .. code-block:: shell

         perf probe -x libarrow.so je_arrow_mallocx '$params' 
         perf probe -x libarrow.so je_arrow_mallocx%return '$retval' 
         perf probe -x libarrow.so je_arrow_rallocx '$params' 
         perf probe -x libarrow.so je_arrow_rallocx%return '$retval' 
         perf probe -x libarrow.so je_arrow_dallocx '$params' 
         PROBE_ARGS="-e probe_libarrow:je_arrow_mallocx \
            -e probe_libarrow:je_arrow_mallocx__return \
            -e probe_libarrow:je_arrow_rallocx \
            -e probe_libarrow:je_arrow_rallocx__return \
            -e probe_libarrow:je_arrow_dallocx"

   .. tab:: mimalloc
      
      .. code-block:: shell

         perf probe -x libarrow.so mi_malloc_aligned '$params' 
         perf probe -x libarrow.so mi_malloc_aligned%return '$retval' 
         perf probe -x libarrow.so mi_realloc_aligned '$params' 
         perf probe -x libarrow.so mi_realloc_aligned%return '$retval' 
         perf probe -x libarrow.so mi_free '$params'
         PROBE_ARGS="-e probe_libarrow:mi_malloc_aligned \
            -e probe_libarrow:mi_malloc_aligned__return \
            -e probe_libarrow:mi_realloc_aligned \
            -e probe_libarrow:mi_realloc_aligned__return \
            -e probe_libarrow:mi_free"

Once probes have been set, you can record calls with associated tracebacks using
``perf record``. In this example, we are running the StructArray unit tests in
Arrow:

.. code-block:: shell

   perf record -g --call-graph dwarf \
     $PROBE_ARGS \
     ./arrow-array-test --gtest_filter=StructArray*

If you want to profile a running process, you can run ``perf record -p <PID>``
and it will record until you interrupt with CTRL+C. Alternatively, you can do
``perf record -P <PID> sleep 10`` to record for 10 seconds.

The resulting data can be processed with standard tools to work with perf or 
``perf script`` can be used to pipe a text format of the data to custom scripts.
The following script parses ``perf script`` output and prints the output in 
new lines delimited JSON for easier processing.

.. code-block:: python
   :caption: process_perf_events.py

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


Here's an example invocation of that script, with a preview of output data:

.. code-block:: console

   $ perf script | python3 /arrow/process_perf_events.py > processed_events.jsonl
   $ head processed_events.jsonl | cut -c -120
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


From there one can answer a number of questions. For example, the following
script will find which allocations were never freed, and print the associated 
tracebacks along with the count of dangling allocations:

.. code-block:: python
   :caption: count_tracebacks.py

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
       elif data['event'] == "probe_libarrow:je_arrow_rallocx":
           address = data['params']['ptr']
           del allocated[address]
       elif data['event'] == "probe_libarrow:je_arrow_rallocx__return":
           address = data['params']['arg1']
           allocated[address] = data['traceback']
       elif data['event'] == "probe_libarrow:je_arrow_dallocx":
           address = data['params']['ptr']
           if address in allocated:
               del allocated[address]
       elif data['event'] == "probe_libarrow:mi_malloc_aligned__return":
           address = data['params']['arg1']
           allocated[address] = data['traceback']
       elif data['event'] == "probe_libarrow:mi_realloc_aligned":
           address = data['params']['p']
           del allocated[address]
       elif data['event'] == "probe_libarrow:mi_realloc_aligned__return":
           address = data['params']['arg1']
           allocated[address] = data['traceback']
       elif data['event'] == "probe_libarrow:mi_free":
           address = data['params']['p']
           if address in allocated:
               del allocated[address]

   traceback_counts = defaultdict(int)

   for traceback in allocated.values():
       traceback_counts[traceback] += 1

   for traceback, count in sorted(traceback_counts.items(), key=lambda x: -x[1]):
       print("Num of dangling allocations:", count)
       print(traceback)


The script can be invoked like so:

.. code-block:: console

   $ cat processed_events.jsonl | python3 /arrow/count_tracebacks.py
   Num of dangling allocations: 1
    7fc945e5cfd2 arrow::(anonymous namespace)::JemallocAllocator::ReallocateAligned+0x13b (/build/cpp/debug/libarrow.so.700.0.0)
    7fc945e5fe4f arrow::BaseMemoryPoolImpl<arrow::(anonymous namespace)::JemallocAllocator>::Reallocate+0x93 (/build/cpp/debug/libarrow.so.700.0.0)
    7fc945e618f7 arrow::PoolBuffer::Resize+0xed (/build/cpp/debug/libarrow.so.700.0.0)
    55a38b163859 arrow::BufferBuilder::Resize+0x12d (/build/cpp/debug/arrow-array-test)
    55a38b163bbe arrow::BufferBuilder::Finish+0x48 (/build/cpp/debug/arrow-array-test)
    55a38b163e3a arrow::BufferBuilder::Finish+0x50 (/build/cpp/debug/arrow-array-test)
    55a38b163f90 arrow::BufferBuilder::FinishWithLength+0x4e (/build/cpp/debug/arrow-array-test)
    55a38b2c8fa7 arrow::TypedBufferBuilder<int, void>::FinishWithLength+0x4f (/build/cpp/debug/arrow-array-test)
    55a38b2bcce7 arrow::NumericBuilder<arrow::Int32Type>::FinishInternal+0x107 (/build/cpp/debug/arrow-array-test)
    7fc945c065ae arrow::ArrayBuilder::Finish+0x5a (/build/cpp/debug/libarrow.so.700.0.0)
    7fc94736ed41 arrow::ipc::internal::json::(anonymous namespace)::Converter::Finish+0x123 (/build/cpp/debug/libarrow.so.700.0.0)
    7fc94737426e arrow::ipc::internal::json::ArrayFromJSON+0x299 (/build/cpp/debug/libarrow.so.700.0.0)
    7fc948e98858 arrow::ArrayFromJSON+0x64 (/build/cpp/debug/libarrow_testing.so.700.0.0)
    55a38b6773f3 arrow::StructArray_FlattenOfSlice_Test::TestBody+0x79 (/build/cpp/debug/arrow-array-test)
    7fc944689633 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::Test, void>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc94468132a testing::internal::HandleExceptionsInMethodIfSupported<testing::Test, void>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc9446555eb testing::Test::Run+0xf1 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc94465602d testing::TestInfo::Run+0x13f (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc944656947 testing::TestSuite::Run+0x14b (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc9446663f5 testing::internal::UnitTestImpl::RunAllTests+0x433 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc94468ab61 testing::internal::HandleSehExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x68 (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc944682568 testing::internal::HandleExceptionsInMethodIfSupported<testing::internal::UnitTestImpl, bool>+0x5d (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc944664b0c testing::UnitTest::Run+0xcc (/build/cpp/googletest_ep-prefix/lib/libgtestd.so.1.11.0)
    7fc9446d0299 RUN_ALL_TESTS+0x14 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
    7fc9446d021b main+0x42 (/build/cpp/googletest_ep-prefix/lib/libgtest_maind.so.1.11.0)
    7fc9441e70b2 __libc_start_main+0xf2 (/usr/lib/x86_64-linux-gnu/libc-2.31.so)
    55a38b10a50d _start+0x2d (/build/cpp/debug/arrow-array-test)
