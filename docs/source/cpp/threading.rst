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

.. _cpp_thread_management:

=================
Thread Management
=================

.. seealso::
   :doc:`Thread management API reference <api/thread>`

Thread Pools
============

Many Arrow C++ operations distribute work across multiple threads to take
advantage of underlying hardware parallelism.  For example, when :func:`reading a
Parquet file<parquet::arrow::FileReader::set_use_threads>` we can decode each
column in parallel.  To achieve this we submit tasks to an executor of some kind.

Within Arrow C++ we use thread pools for parallel scheduling and an event loop
when the user has requested serial execution.  It is possible for
users to provide their own custom implementation, though that is an advanced
concept and not covered here.

CPU vs. I/O
-----------

In order to minimize the overhead of context switches our default thread pool
for CPU-intensive tasks has a fixed size, defaulting to the process CPU affinity (on Linux) or
`std::thread::hardware_concurrency <https://en.cppreference.com/w/cpp/thread/thread/hardware_concurrency>`_.
This means that CPU tasks should never block for long periods of time because this
will result in under-utilization of the CPU.  To achieve this we have a separate
thread pool which should be used for tasks that need to block.  Since these tasks
are usually associated with I/O operations we call this the I/O thread pool.  This
model is often associated with asynchronous computation.

.. _io_thread_pool:

The size of the I/O thread pool currently defaults to 8 threads and should
be sized according to the parallel capabilities of the I/O hardware.  For example,
if most reads and writes occur on a typical HDD then the default of 8 will probably
be sufficient.  On the other hand, when most reads and writes occur on a remote
filesystem such as S3, it is often possible to benefit from many concurrent reads
and it may be possible to increase I/O performance by increasing the size of the
I/O thread pool.  The size of the default I/O thread pool can be managed with
the :envvar:`ARROW_IO_THREADS` environment variable or
with the :func:`arrow::io::SetIOThreadPoolCapacity` function.

Increasing the size of the CPU thread pool is not likely to have any benefit.  In
some cases it may make sense to decrease the size of the CPU thread pool in order
to reduce the impact that Arrow C++ has on hardware shared with other processes or user
threads.  The size of the default CPU thread pool can be managed with the
:envvar:`OMP_NUM_THREADS` environment variable or with the
:func:`arrow::SetCpuThreadPoolCapacity` function.

Serial Execution
----------------

Operations in Arrow C++ that may use threads can usually be configured to run serially
via some kind of parameter.  In this case we typically replace the CPU executor with
an event loop operated by the calling thread.  However, many operations will continue
to use the I/O thread pool.  This means that some parallelism may still occur even when
serial execution is requested.

Jemalloc Background Threads
---------------------------

When using the :ref:`jemalloc allocator<cpp_memory_pool>` a small number of
background threads will be created by jemalloc to manage the pool.  These threads
should have minimal impact but can show up as a memory leak when running analysis
tools like Valgrind.  This is harmless and can be safely suppressed or Arrow C++ can
be compiled without jemalloc.

Asynchronous Utilities
======================

Future
------

Arrow C++ uses :class:`arrow::Future` to communicate results between threads.  Typically
an :class:`arrow::Future` will be created when an operation needs to perform some kind
of long running task that will block for some period of time.  :class:`arrow::Future`
objects are mainly meant for internal use and any method that returns an
:class:`arrow::Future` will usually have a synchronous variant as well.
