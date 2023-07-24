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

.. highlight:: console
.. _development-cpp:

======================
Development Guidelines
======================

This section provides information for developers who wish to contribute to the
C++ codebase.

.. note::

   Since most of the project's developers work on Linux or macOS, not all
   features or developer tools are uniformly supported on Windows. If you are
   on Windows, have a look at :ref:`developers-cpp-windows`.

Compiler warning levels
=======================

The ``BUILD_WARNING_LEVEL`` CMake option switches between sets of predetermined
compiler warning levels that we use for code tidiness. For release builds, the
default warning level is ``PRODUCTION``, while for debug builds the default is
``CHECKIN``.

When using ``CHECKIN`` for debug builds, ``-Werror`` is added when using gcc
and clang, causing build failures for any warning, and ``/WX`` is set with MSVC
having the same effect.

Running unit tests
==================

The ``-DARROW_BUILD_TESTS=ON`` CMake option enables building of unit test
executables.  You can then either run them individually, by launching the
desired executable, or run them all at once by launching the ``ctest``
executable (which is part of the CMake suite).

A possible invocation is something like::

   $ ctest -j16 --output-on-failure

where the ``-j16`` option runs up to 16 tests in parallel, taking advantage
of multiple CPU cores and hardware threads.

Running benchmarks
==================

The ``-DARROW_BUILD_BENCHMARKS=ON`` CMake option enables building of benchmark
executables.  You can then run benchmarks individually by launching the
corresponding executable from the command line, e.g.::

   $ ./build/release/arrow-builder-benchmark

.. note::
   For meaningful benchmark numbers, it is very strongly recommended to build
   in ``Release`` mode, so as to enable compiler optimizations.

Code Style, Linting, and CI
===========================

This project follows `Google's C++ Style Guide
<https://google.github.io/styleguide/cppguide.html>`_ with these exceptions:

* We relax the line length restriction to 90 characters.
* We use the ``NULLPTR`` macro in header files (instead of ``nullptr``) defined
  in ``src/arrow/util/macros.h`` to support building C++/CLI (ARROW-1134).
* We relax the guide's rules regarding structs.  For public headers we should
  use struct only for objects that are principally simple data containers where
  it is OK to expose all the internal members and any methods are primarily
  conveniences.  For private headers the rules are relaxed further and structs
  can be used where convenient for types that do not need access control even
  though they may not be simple data containers.
* We prefer pointers for output and input/output parameters (the
  style guide recommends mutable references in some cases).

Our continuous integration builds on GitHub Actions run the unit test
suites on a variety of platforms and configuration, including using
Address Sanitizer and Undefined Behavior Sanitizer to check for various
patterns of misbehaviour such as memory leaks. In addition, the
codebase is subjected to a number of code style and code cleanliness checks.

In order to have a passing CI build, your modified Git branch must pass the
following checks:

* C++ builds with the project's active version of ``clang`` without
  compiler warnings with ``-DBUILD_WARNING_LEVEL=CHECKIN``. Note that
  there are classes of warnings (such as ``-Wdocumentation``, see more
  on this below) that are not caught by ``gcc``.
* Passes various C++ (and others) style checks, checked with the ``lint``
  subcommand to :ref:`Archery <archery>`. This can also be fixed locally
  by running ``archery lint --cpplint --clang-format --clang-tidy --fix``.
* CMake files pass style checks, can be fixed by running
  ``archery lint --cmake-format --fix``. This requires Python
  3 and `cmake_format <https://github.com/cheshirekow/cmake_format>`_ (note:
  this currently does not work on Windows).

On pull requests, the "Dev / Lint" pipeline will run these checks, and report
what files/lines need to be fixed, if any.

In order to account for variations in the behavior of ``clang-format`` between
major versions of LLVM, we pin the version of ``clang-format`` used. You can
confirm the current pinned version by finding the ``CLANG_TOOLS`` variable
value in `.env <https://github.com/apache/arrow/blob/main/.env>`_. Note that
the version must match exactly; a newer version (even a patch release) will
not work. LLVM can be installed through a system package manager or a package
manager like Conda or Homebrew, though note they may not offer the exact
version needed. Alternatively, binaries can be directly downloaded from the
`LLVM website <https://releases.llvm.org/>`_.

For convenience, C++ style checks can run via a build, in addition to
Archery. To do so, build one or more of the targets ``format`` (for
clang-format), ``lint_cpp_cli``, ``lint`` (for cpplint), or
``clang-tidy``. For example::

  $ cmake -GNinja ../cpp ...
  $ ninja format lint clang-tidy lint_cpp_cli

Depending on how you installed clang-format, the build system may not be able
to find it. In that case, invoking CMake will show errors like the following::

  -- clang-format 12 not found

Or if the wrong version is installed::

  -- clang-format found, but version did not match "^clang-format version 12"

You can provide an explicit path to the directory containing the clang-format
executable and others with the environment variable ``$CLANG_TOOLS_PATH``, or
by passing ``-DClangTools_PATH=$PATH_TO_CLANG_TOOLS`` when invoking CMake. For
example::

  # We unpacked LLVM here:
  $ ~/tools/bin/clang-format --version
  clang-format version 12.0.0
  # Pass the directory containing the tools to CMake
  $ cmake ../cpp -DClangTools_PATH=~/tools/bin/
  ...snip...
  -- clang-tidy found at /home/user/tools/bin/clang-tidy
  -- clang-format found at /home/user/tools/bin/clang-format
  ...snip...

To make linting more reproducible for everyone, we provide a ``docker-compose``
target that is executable from the root of the repository:

.. code-block::

   $ docker-compose run ubuntu-lint

Alternatively, on an open pull request, the comment bot can format C++ code
for you (it will push a commit to the branch that can then be pulled). Just
comment the following::

  @github-actions autotune

Cleaning includes with include-what-you-use (IWYU)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We occasionally use Google's `include-what-you-use
<https://github.com/include-what-you-use/include-what-you-use>`_ tool, also
known as IWYU, to remove unnecessary imports.

To begin using IWYU, you must first build it by following the instructions in
the project's documentation. Once the ``include-what-you-use`` executable is in
your ``$PATH``, you must run CMake with ``-DCMAKE_EXPORT_COMPILE_COMMANDS=ON``
in a new out-of-source CMake build directory like so:

.. code-block:: shell

   mkdir -p $ARROW_ROOT/cpp/iwyu
   cd $ARROW_ROOT/cpp/iwyu
   cmake -DCMAKE_EXPORT_COMPILE_COMMANDS=ON \
     -DARROW_BUILD_BENCHMARKS=ON \
     -DARROW_BUILD_BENCHMARKS_REFERENCE=ON \
     -DARROW_BUILD_TESTS=ON \
     -DARROW_BUILD_UTILITIES=ON \
     -DARROW_COMPUTE=ON \
     -DARROW_CSV=ON \
     -DARROW_DATASET=ON \
     -DARROW_FILESYSTEM=ON \
     -DARROW_FLIGHT=ON \
     -DARROW_GANDIVA=ON \
     -DARROW_HDFS=ON \
     -DARROW_JSON=ON \
     -DARROW_PARQUET=ON \
     -DARROW_S3=ON \
     -DARROW_WITH_BROTLI=ON \
     -DARROW_WITH_BZ2=ON \
     -DARROW_WITH_LZ4=ON \
     -DARROW_WITH_SNAPPY=ON \
     -DARROW_WITH_ZLIB=ON \
     -DARROW_WITH_ZSTD=ON \
     ..

In order for IWYU to run on the desired component in the codebase, it must be
enabled by the CMake configuration flags. Once this is done, you can run IWYU
on the whole codebase by running a helper ``iwyu.sh`` script:

.. code-block:: shell

   IWYU_SH=$ARROW_ROOT/cpp/build-support/iwyu/iwyu.sh
   ./$IWYU_SH

Since this is very time consuming, you can check a subset of files matching
some string pattern with the special "match" option

.. code-block:: shell

   ./$IWYU_SH match $PATTERN

For example, if you wanted to do IWYU checks on all files in
``src/arrow/array``, you could run

.. code-block:: shell

   ./$IWYU_SH match arrow/array

Checking for ABI and API stability
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To build ABI compliance reports, you need to install the two tools
``abi-dumper`` and ``abi-compliance-checker``.

Build Arrow C++ in Debug mode, alternatively you could use ``-Og`` which also
builds with the necessary symbols but includes a bit of code optimization.
Once the build has finished, you can generate ABI reports using:

.. code-block:: shell

   abi-dumper -lver 9 debug/libarrow.so -o ABI-9.dump

The above version number is freely selectable. As we want to compare versions,
you should now ``git checkout`` the version you want to compare it to and re-run
the above command using a different version number. Once both reports are
generated, you can build a comparison report using

.. code-block:: shell

   abi-compliance-checker -l libarrow -d1 ABI-PY-9.dump -d2 ABI-PY-10.dump

The report is then generated in ``compat_reports/libarrow`` as a HTML.

API Documentation
=================

We use Doxygen style comments (``///``) in header files for comments
that we wish to show up in API documentation for classes and
functions.

When using ``clang`` and building with
``-DBUILD_WARNING_LEVEL=CHECKIN``, the ``-Wdocumentation`` flag is
used which checks for some common documentation inconsistencies, like
documenting some, but not all function parameters with ``\param``. See
the `LLVM documentation warnings section
<https://releases.llvm.org/7.0.1/tools/clang/docs/DiagnosticsReference.html#wdocumentation>`_
for more about this.

While we publish the API documentation as part of the main Sphinx-based
documentation site, you can also build the C++ API documentation anytime using
Doxygen. Run the following command from the ``cpp/apidoc`` directory:

.. code-block:: shell

   doxygen Doxyfile

This requires `Doxygen <https://www.doxygen.org>`_ to be installed.

Apache Parquet Development
==========================

To build the C++ libraries for Apache Parquet, add the flag
``-DARROW_PARQUET=ON`` when invoking CMake.
To build Apache Parquet with encryption support, add the flag
``-DPARQUET_REQUIRE_ENCRYPTION=ON`` when invoking CMake. The Parquet libraries and unit tests
can be built with the ``parquet`` make target:

.. code-block:: shell

   make parquet

On Linux and macOS if you do not have Apache Thrift installed on your system,
or you are building with ``-DThrift_SOURCE=BUNDLED``, you must install
``bison`` and ``flex`` packages. On Windows we handle these build dependencies
automatically when building Thrift from source.

Running ``ctest -L unittest`` will run all built C++ unit tests, while ``ctest -L
parquet`` will run only the Parquet unit tests. The unit tests depend on an
environment variable ``PARQUET_TEST_DATA`` that depends on a git submodule to the
repository https://github.com/apache/parquet-testing:

.. code-block:: shell

   git submodule update --init
   export PARQUET_TEST_DATA=$ARROW_ROOT/cpp/submodules/parquet-testing/data

Here ``$ARROW_ROOT`` is the absolute path to the Arrow codebase.

Arrow Flight RPC
================

In addition to the Arrow dependencies, Flight requires:

* gRPC (>= 1.14, roughly)
* Protobuf (>= 3.6, earlier versions may work)
* c-ares (used by gRPC)

By default, Arrow will try to download and build these dependencies
when building Flight.

The optional ``flight`` libraries and tests can be built by passing
``-DARROW_FLIGHT=ON``.

.. code-block:: shell

   cmake .. -DARROW_FLIGHT=ON -DARROW_BUILD_TESTS=ON
   make

You can also use existing installations of the extra dependencies.
When building, set the environment variables ``gRPC_ROOT`` and/or
``Protobuf_ROOT`` and/or ``c-ares_ROOT``.

We are developing against recent versions of gRPC, and the versions. The
``grpc-cpp`` package available from https://conda-forge.org/ is one reliable
way to obtain gRPC in a cross-platform way. You may try using system libraries
for gRPC and Protobuf, but these are likely to be too old. On macOS, you can
try `Homebrew <https://brew.sh/>`_:

.. code-block:: shell

   brew install grpc
