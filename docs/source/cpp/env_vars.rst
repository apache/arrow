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

.. _cpp_env_vars:

=====================
Environment Variables
=====================

The following environment variables can be used to affect the behavior of
Arrow C++ at runtime.  Many of these variables are inspected only once per
process (for example, when the Arrow C++ DLL is loaded), so you cannot assume
that changing their value later will have an effect.

.. envvar:: ARROW_DEBUG_MEMORY_POOL

   Enable rudimentary memory checks to guard against buffer overflows.
   The value of this environment variable selects the behavior when a
   buffer overflow is detected:

   - ``abort`` exits the processus with a non-zero return value;
   - ``trap`` issues a platform-specific debugger breakpoint / trap instruction;
   - ``warn`` prints a warning on stderr and continues execution;

   If this variable is not set, or has empty an value, memory checks are disabled.

   .. note::
      While this functionality can be useful and has little overhead, it
      is not a replacement for more sophisticated memory checking utilities
      such as `Valgrind <https://valgrind.org/>`_ or
      `Address Sanitizer <https://clang.llvm.org/docs/AddressSanitizer.html>`_.

.. envvar:: ARROW_DEFAULT_MEMORY_POOL

   Override the backend to be used for the default
   :ref:`memory pool <cpp_memory_pool>`. Possible values are among ``jemalloc``,
   ``mimalloc`` and ``system``, depending on which backends were enabled when
   :ref:`building Arrow C++ <building-arrow-cpp>`.

.. envvar:: ARROW_IO_THREADS

   Override the default number of threads for the global IO thread pool.
   The value of this environment variable should be a positive integer.

.. envvar:: ARROW_LIBHDFS_DIR

   The directory containing the C HDFS library (``hdfs.dll`` on Windows,
   ``libhdfs.dylib`` on macOS, ``libhdfs.so`` on other platforms).
   Alternatively, one can set :envvar:`HADOOP_HOME`.

.. envvar:: ARROW_TRACING_BACKEND

   The backend where to export `OpenTelemetry <https://opentelemetry.io/>`_-based
   execution traces.  Possible values are:

   - ``ostream``: emit textual log messages to stdout;
   - ``otlp_http``: emit OTLP JSON encoded traces to a HTTP server (by default,
     the endpoint URL is "http://localhost:4318/v1/traces");
   - ``arrow_otlp_stdout``: emit JSON traces to stdout;
   - ``arrow_otlp_stderr``: emit JSON traces to stderr.

   If this variable is not set, no traces are exported.

   This environment variable has no effect if Arrow C++ was not built with
   tracing enabled.

   .. seealso::

      `OpenTelemetry configuration for remote endpoints
      <https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/protocol/exporter.md>`__

.. envvar:: ARROW_USER_SIMD_LEVEL

   The SIMD optimization level to select.  By default, Arrow C++ detects
   the capabilities of the current CPU at runtime and chooses the best
   execution paths based on that information.  One can override the detection
   by setting this environment variable to a well-defined value.
   Supported values are:

   - ``NONE`` disables any runtime-selected SIMD optimization;
   - ``SSE4_2`` enables any SSE2-based optimizations until SSE4.2 (included);
   - ``AVX`` enables any AVX-based optimizations and earlier;
   - ``AVX2`` enables any AVX2-based optimizations and earlier;
   - ``AVX512`` enables any AVX512-based optimizations and earlier.

   This environment variable only has an effect on x86 platforms.  Other
   platforms currently do not implement any form of runtime dispatch.

   .. note::
      In addition to runtime dispatch, the compile-time SIMD level can
      be set using the ``ARROW_SIMD_LEVEL`` CMake configuration variable.
      Unlike runtime dispatch, compile-time SIMD optimizations cannot be
      changed at runtime (for example, if you compile Arrow C++ with AVX512
      enabled, the resulting binary will only run on AVX512-enabled CPUs).
      Setting ``ARROW_USER_SIMD_LEVEL=NONE`` prevents the execution of
      explicit SIMD optimization code, but it does not rule out the execution
      of compiler generated SIMD instructions.  E.g., on x86_64 platform,
      Arrow is built with ``ARROW_SIMD_LEVEL=SSE4_2`` by default.  Compiler
      may generate SSE4.2 instructions from any C/C++ source code.  On legacy
      x86_64 platforms do not support SSE4.2, Arrow binary may fail with
      SIGILL (Illegal Instruction).  User must rebuild Arrow and PyArrow from
      scratch by setting cmake option ``ARROW_SIMD_LEVEL=NONE``.

.. envvar:: GANDIVA_CACHE_SIZE

   The number of entries to keep in the Gandiva JIT compilation cache.
   The cache is in-memory and does not persist accross processes.

.. envvar:: HADOOP_HOME

   The path to the Hadoop installation.

.. envvar:: JAVA_HOME

   Set the path to the Java Runtime Environment installation. This may be
   required for HDFS support if Java is installed in a non-standard location.

.. envvar:: OMP_NUM_THREADS

   The number of worker threads in the global (process-wide) CPU thread pool.
   If this environment variable is not defined, the available hardware
   concurrency is determined using a platform-specific routine.

.. envvar:: OMP_THREAD_LIMIT

   An upper bound for the number of worker threads in the global
   (process-wide) CPU thread pool.

   For example, if the current machine has 4 hardware threads and
   ``OMP_THREAD_LIMIT`` is 8, the global CPU thread pool will have 4 worker
   threads.  But if ``OMP_THREAD_LIMIT`` is 2, the global CPU thread pool
   will have 2 worker threads.
