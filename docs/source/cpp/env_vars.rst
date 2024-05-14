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

.. envvar:: ACERO_ALIGNMENT_HANDLING

   Arrow C++'s Acero module performs computation on streams of data.  This
   computation may involve a form of "type punning" that is technically
   undefined behavior if the underlying array is not properly aligned.  On
   most modern CPUs this is not an issue, but some older CPUs may crash or
   suffer poor performance.  For this reason it is recommended that all
   incoming array buffers are properly aligned, but some data sources
   such as :ref:`Flight <flight-rpc>` may produce unaligned buffers.

   The value of this environment variable controls what will happen when
   Acero detects an unaligned buffer:

   - ``warn``: a warning is emitted
   - ``ignore``: nothing, alignment checking is disabled
   - ``reallocate``: the buffer is reallocated to a properly aligned address
   - ``error``: the operation fails with an error

   The default behavior is ``warn``.  On modern hardware it is usually safe
   to change this to ``ignore``.  Changing to ``reallocate`` is the safest
   option but this will have a significant performance impact as the buffer
   will need to be copied.

.. envvar:: ARROW_DEBUG_MEMORY_POOL

   Enable rudimentary memory checks to guard against buffer overflows.
   The value of this environment variable selects the behavior when a
   buffer overflow is detected:

   - ``abort`` exits the processus with a non-zero return value;
   - ``trap`` issues a platform-specific debugger breakpoint / trap instruction;
   - ``warn`` prints a warning on stderr and continues execution;
   - ``none`` disables memory checks;

   If this variable is not set, or has an empty value, it has the same effect
   as the value ``none`` - memory checks are disabled.

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

.. envvar:: ARROW_S3_LOG_LEVEL

   Controls the verbosity of logging produced by S3 calls. Defaults to ``FATAL``
   which only produces output in the case of fatal errors. ``DEBUG`` is recommended
   when you're trying to troubleshoot issues.

   Possible values include:

   - ``FATAL`` (the default)
   - ``ERROR``
   - ``WARN``
   - ``INFO``
   - ``DEBUG``
   - ``TRACE``
   - ``OFF``

   .. seealso::

      `Logging - AWS SDK For C++
      <https://docs.aws.amazon.com/sdk-for-cpp/v1/developer-guide/logging.html>`__


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

   The maximum SIMD optimization level selectable at runtime.  Useful for
   comparing the performance impact of enabling or disabling respective code
   paths or working around situations where instructions are supported but are
   not performant or cause other issues.

   By default, Arrow C++ detects the capabilities of the current CPU at runtime
   and chooses the best execution paths based on that information.  This
   behavior can be overriden by setting this environment variable to a
   well-defined value.  Supported values are:

   - ``NONE`` disables any runtime-selected SIMD optimization;
   - ``SSE4_2`` enables any SSE2-based optimizations until SSE4.2 (included);
   - ``AVX`` enables any AVX-based optimizations and earlier;
   - ``AVX2`` enables any AVX2-based optimizations and earlier;
   - ``AVX512`` enables any AVX512-based optimizations and earlier.

   This environment variable only has an effect on x86 platforms.  Other
   platforms currently do not implement any form of runtime dispatch.

   .. note::
      In addition to runtime-selected SIMD optimizations dispatch, Arrow C++ can
      also be compiled with SIMD optimizations that cannot be disabled at
      runtime.  For example, by default, SSE4.2 optimizations are enabled on x86
      builds: therefore, with this default setting, Arrow C++ does not work at
      all on a CPU without support for SSE4.2.  This setting can be changed
      using the ``ARROW_SIMD_LEVEL`` CMake variable so as to either raise or
      lower the optimization level.

      Finally, the ``ARROW_RUNTIME_SIMD_LEVEL`` CMake variable sets a
      compile-time upper bound to runtime-selected SIMD optimizations.  This is
      useful in cases where a compiler reports support for an instruction set
      but does not actually support it in full.

.. envvar:: AWS_ENDPOINT_URL

   Endpoint URL used for S3-like storage, for example Minio or s3.scality.
   Alternatively, one can set :envvar:`AWS_ENDPOINT_URL_S3`.

.. envvar:: AWS_ENDPOINT_URL_S3

   Endpoint URL used for S3-like storage, for example Minio or s3.scality.
   This takes precedence over :envvar:`AWS_ENDPOINT_URL` if both variables
   are set.

.. envvar:: GANDIVA_CACHE_SIZE

   The number of entries to keep in the Gandiva JIT compilation cache.
   The cache is in-memory and does not persist across processes.

   The default cache size is 5000.  The value of this environment variable
   should be a positive integer and should not exceed the maximum value
   of int32.  Otherwise the default value is used.

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
