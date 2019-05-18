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

.. _cpp-development:

***************
C++ Development
***************

System setup
============

Arrow uses CMake as a build configuration system. We recommend building
out-of-source. If you are not familiar with this terminology:

* **In-source build**: ``cmake`` is invoked directly from the ``cpp``
  directory. This can be inflexible when you wish to maintain multiple build
  environments (e.g. one for debug builds and another for release builds)
* **Out-of-source build**: ``cmake`` is invoked from another directory,
  creating an isolated build environment that does not interact with any other
  build environment. For example, you could create ``cpp/build-debug`` and
  invoke ``cmake $CMAKE_ARGS ..`` from this directory

Building requires:

* A C++11-enabled compiler. On Linux, gcc 4.8 and higher should be
  sufficient. For Windows, at least Visual Studio 2015 is required.
* CMake 3.2 or higher
* Boost
* ``bison`` and ``flex`` (for building Apache Thrift from source only, an
  Apache Parquet dependency.)

Running the unit tests using ``ctest`` requires:

* python

On Ubuntu/Debian you can install the requirements with:

.. code-block:: shell

   sudo apt-get install \
        autoconf \
        build-essential \
        cmake \
        libboost-dev \
        libboost-filesystem-dev \
        libboost-regex-dev \
        libboost-system-dev \
        python \
        bison \
        flex

On Alpine Linux:

.. code-block:: shell

   apk add autoconf \
           bash \
           boost-dev \
           cmake \
           g++ \
           gcc \
           make

On macOS, you can use `Homebrew <https://brew.sh/>`_.

.. code-block:: shell

   git clone https://github.com/apache/arrow.git
   cd arrow
   brew update && brew bundle --file=cpp/Brewfile

Building
========

The build system uses ``CMAKE_BUILD_TYPE=release`` by default, so if this
argument is omitted then a release build will be produced.

Minimal release build:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git
   cd arrow/cpp
   mkdir release
   cd release
   cmake -DARROW_BUILD_TESTS=ON  ..
   make unittest

Minimal debug build:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git
   cd arrow/cpp
   mkdir debug
   cd debug
   cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_BUILD_TESTS=ON ..
   make unittest

If you do not need to build the test suite, you can omit the
``ARROW_BUILD_TESTS`` option (the default is not to build the unit tests).

On some Linux distributions, running the test suite might require setting an
explicit locale. If you see any locale-related errors, try setting the
environment variable (which requires the `locales` package or equivalent):

.. code-block:: shell

   export LC_ALL="en_US.UTF-8"

Faster builds with Ninja
~~~~~~~~~~~~~~~~~~~~~~~~

Many contributors use the `Ninja build system <https://ninja-build.org/>`_ to
get faster builds. It especially speeds up incremental builds. To use
``ninja``, pass ``-GNinja`` when calling ``cmake`` and then use the ``ninja``
command instead of ``make``.

Optional Components
~~~~~~~~~~~~~~~~~~~

By default, the C++ build system creates a fairly minimal build. We have
several optional system components which you can opt into building by passing
boolean flags to ``cmake``.

* ``-DARROW_CUDA=ON``: CUDA integration for GPU development. Depends on NVIDIA
  CUDA toolkit. The CUDA toolchain used to build the library can be customized
  by using the ``$CUDA_HOME`` environment variable.
* ``-DARROW_FLIGHT=ON``: Arrow Flight RPC system, which depends at least on
  gRPC
* ``-DARROW_GANDIVA=ON``: Gandiva expression compiler, depends on LLVM,
  Protocol Buffers, and re2
* ``-DARROW_GANDIVA_JAVA=ON``: Gandiva JNI bindings for Java
* ``-DARROW_HDFS=ON``: Arrow integration with libhdfs for accessing the Hadoop
  Filesystem
* ``-DARROW_HIVESERVER2=ON``: Client library for HiveServer2 database protocol
* ``-DARROW_ORC=ON``: Arrow integration with Apache ORC
* ``-DARROW_PARQUET=ON``: Apache Parquet libraries and Arrow integration
* ``-DARROW_PLASMA=ON``: Plasma Shared Memory Object Store
* ``-DARROW_PLASMA_JAVA_CLIENT=ON``: Build Java client for Plasma
* ``-DARROW_PYTHON=ON``: Arrow Python C++ integration library (required for
  building pyarrow). This library must be built against the same Python version
  for which you are building pyarrow, e.g. Python 2.7 or Python 3.6. NumPy must
  also be installed.

Some features of the core Arrow shared library can be switched off for improved
build times if they are not required for your application:

* ``-DARROW_COMPUTE=ON``: build the in-memory analytics module
* ``-DARROW_IPC=ON``: build the IPC extensions (requiring Flatbuffers)

CMake version requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~

While we support CMake 3.2 and higher, some features require a newer version of
CMake:

* Building the benchmarks requires 3.6 or higher
* Building zstd from source requires 3.7 or higher
* Building Gandiva JNI bindings requires 3.11 or higher

LLVM and Clang Tools
~~~~~~~~~~~~~~~~~~~~

We are currently using LLVM 7 for library builds and for other developer tools
such as code formatting with ``clang-format``. LLVM can be installed via most
modern package managers (apt, yum, conda, Homebrew, chocolatey).

.. _cpp-build-dependency-management:

Build Dependency Management
===========================

The build system supports a number of third-party dependencies

  * ``BOOST``: for cross-platform support
  * ``BROTLI``: for data compression
  * ``double-conversion``: for text-to-numeric conversions
  * ``Snappy``: for data compression
  * ``gflags``: for command line utilities (formerly Googleflags)
  * ``glog``: for logging
  * ``Thrift``: Apache Thrift, for data serialization
  * ``Protobuf``: Google Protocol Buffers, for data serialization
  * ``GTEST``: Googletest, for testing
  * ``benchmark``: Google benchmark, for testing
  * ``RapidJSON``: for data serialization
  * ``Flatbuffers``: for data serialization
  * ``ZLIB``: for data compression
  * ``BZip2``: for data compression
  * ``LZ4``: for data compression
  * ``ZSTD``: for data compression
  * ``RE2``: for regular expressions
  * ``gRPC``: for remote procedure calls
  * ``c-ares``: a dependency of gRPC
  * ``LLVM``: a dependency of Gandiva

The CMake option ``ARROW_DEPENDENCY_SOURCE`` is a global option that instructs
the build system how to resolve each dependency. There are a few options:

* ``AUTO``: try to find package in the system default locations and build from
  source if not found
* ``BUNDLED``: Building the dependency automatically from source
* ``SYSTEM``: Finding the dependency in system paths using CMake's built-in
  ``find_package`` function, or using ``pkg-config`` for packages that do not
  have this feature
* ``BREW``: Use Homebrew default paths as an alternative ``SYSTEM`` path
* ``CONDA``: Use ``$CONDA_PREFIX`` as alternative ``SYSTEM`` PATH

The default method is ``AUTO`` unless you are developing within an active conda
environment (detected by presence of the ``$CONDA_PREFIX`` environment
variable), in which case it is ``CONDA``.

Individual Dependency Resolution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

While ``-DARROW_DEPENDENCY_SOURCE=$SOURCE`` sets a global default for all
packages, the resolution strategy can be overridden for individual packages by
setting ``-D$PACKAGE_NAME_SOURCE=..``. For example, to build Protocol Buffers
from source, set

.. code-block:: shell

   -DProtobuf_SOURCE=BUNDLED

This variable is unfortunately case-sensitive; the name used for each package
is listed above, but the most up-to-date listing can be found in
`cpp/cmake_modules/ThirdpartyToolchain.cmake <https://github.com/apache/arrow/blob/master/cpp/cmake_modules/ThirdpartyToolchain.cmake>`_.

Bundled Dependency Versions
~~~~~~~~~~~~~~~~~~~~~~~~~~~

When using the ``BUNDLED`` method to build a dependency from source, the
version number from ``cpp/thirdparty/versions.txt`` is used. There is also a
dependency source downloader script (see below), which can be used to set up
offline builds.

Boost-related Options
~~~~~~~~~~~~~~~~~~~~~

We depend on some Boost C++ libraries for cross-platform suport. In most cases,
the Boost version available in your package manager may be new enough, and the
build system will find it automatically. If you have Boost installed in a
non-standard location, you can specify it by passing
``-DBOOST_ROOT=$MY_BOOST_ROOT`` or setting the ``BOOST_ROOT`` environment
variable.

Unlike most of the other dependencies, if Boost is not found by the build
system it will not be built automatically from source. To opt-in to a vendored
Boost build, pass ``-DARROW_BOOST_VENDORED=ON``. This automatically sets the
option ``-DARROW_BOOST_USE_SHARED=OFF`` to statically-link Boost into the
produced libraries and executables.

Offline Builds
~~~~~~~~~~~~~~

If you do not use the above variables to direct the Arrow build system to
preinstalled dependencies, they will be built automatically by the Arrow build
system. The source archive for each dependency will be downloaded via the
internet, which can cause issues in environments with limited access to the
internet.

To enable offline builds, you can download the source artifacts yourself and
use environment variables of the form ``ARROW_$LIBRARY_URL`` to direct the
build system to read from a local file rather than accessing the internet.

To make this easier for you, we have prepared a script
``thirdparty/download_dependencies.sh`` which will download the correct version
of each dependency to a directory of your choosing. It will print a list of
bash-style environment variable statements at the end to use for your build
script.

.. code-block:: shell

   # Download tarballs into $HOME/arrow-thirdparty
   $ ./thirdparty/download_dependencies.sh $HOME/arrow-thirdparty

You can then invoke CMake to create the build directory and it will use the
declared environment variable pointing to downloaded archives instead of
downloading them (one for each build dir!).

General C++ Development
=======================

This section provides information for developers who wish to contribute to the
C++ codebase.

.. note::

   Since most of the project's developers work on Linux or macOS, not all
   features or developer tools are uniformly supported on Windows. If you are
   on Windows, have a look at the later section on Windows development.

Compiler warning levels
~~~~~~~~~~~~~~~~~~~~~~~

The ``BUILD_WARNING_LEVEL`` CMake option switches between sets of predetermined
compiler warning levels that we use for code tidiness. For release builds, the
default warning level is ``PRODUCTION``, while for debug builds the default is
``CHECKIN``.

When using ``CHECKIN`` for debug builds, ``-Werror`` is added when using gcc
and clang, causing build failures for any warning, and ``/WX`` is set with MSVC
having the same effect.

Code Style, Linting, and CI
~~~~~~~~~~~~~~~~~~~~~~~~~~~

This project follows `Google's C++ Style Guide
<https://google.github.io/styleguide/cppguide.html>`_ with minor exceptions:

* We relax the line length restriction to 90 characters.
* We use doxygen style comments ("///") in header files for comments that we
  wish to show up in API documentation
* We use the ``NULLPTR`` macro in header files (instead of ``nullptr``) defined
  in ``src/arrow/util/macros.h`` to support building C++/CLI (ARROW-1134)

Our continuous integration builds in Travis CI and Appveyor run the unit test
suites on a variety of platforms and configuration, including using
``valgrind`` to check for memory leaks or bad memory accesses. In addition, the
codebase is subjected to a number of code style and code cleanliness checks.

In order to have a passing CI build, your modified git branch must pass the
following checks:

* C++ builds without compiler warnings with ``-DBUILD_WARNING_LEVEL=CHECKIN``
* C++ unit test suite with valgrind enabled, use ``-DARROW_TEST_MEMCHECK=ON``
  when invoking CMake
* Passes cpplint checks, checked with ``make lint``
* Conforms to ``clang-format`` style, checked with ``make check-format``
* Passes C++/CLI header file checks, invoked with
  ``cpp/build-support/lint_cpp_cli.py cpp/src``
* CMake files pass style checks, can be fixed by running
  ``run-cmake-format.py`` from the root of the repository. This requires Python
  3 and `cmake_format <https://github.com/cheshirekow/cmake_format>`_ (note:
  this currently does not work on Windows)

In order to account for variations in the behavior of ``clang-format`` between
major versions of LLVM, we pin the version of ``clang-format`` used (current
LLVM 7).

Depending on how you installed clang-format, the build system may not be able
to find it. You can provide an explicit path to your LLVM installation (or the
root path for the clang tools) with the environment variable
`$CLANG_TOOLS_PATH` or by passing ``-DClangTools_PATH=$PATH_TO_CLANG_TOOLS`` when
invoking CMake.

To make linting more reproducible for everyone, we provide a ``docker-compose``
target that is executable from the root of the repository:

.. code-block:: shell

   docker-compose run lint

See :ref:`integration` for more information about the project's
``docker-compose`` configuration.

Modular Build Targets
~~~~~~~~~~~~~~~~~~~~~

Since there are several major parts of the C++ project, we have provided
modular CMake targets for building each library component, group of unit tests
and benchmarks, and their dependencies:

* ``make arrow`` for Arrow core libraries
* ``make parquet`` for Parquet libraries
* ``make gandiva`` for Gandiva (LLVM expression compiler) libraries
* ``make plasma`` for Plasma libraries, server

To build the unit tests or benchmarks, add ``-tests`` or ``-benchmarks`` to the
target name. So ``make arrow-tests`` will build the Arrow core unit
tests. Using the ``-all`` target, e.g. ``parquet-all``, will build everything.

If you wish to only build and install one or more project subcomponents, we
have provided the CMake option ``ARROW_OPTIONAL_INSTALL`` to only install
targets that have been built. For example, if you only wish to build the
Parquet libraries, its tests, and its dependencies, you can run:

.. code-block:: shell

   cmake .. -DARROW_PARQUET=ON \
         -DARROW_OPTIONAL_INSTALL=ON \
         -DARROW_BUILD_TESTS=ON
   make parquet
   make install

If you omit an explicit target when invoking ``make``, all targets will be
built.

Building API Documentation
~~~~~~~~~~~~~~~~~~~~~~~~~~

While we publish the API documentation as part of the main Sphinx-based
documentation site, you can also build the C++ API documentation anytime using
Doxygen. Run the following command from the ``cpp/apidoc`` directory:

.. code-block:: shell

   doxygen Doxyfile

This requires `Doxygen <https://www.doxygen.org>`_ to be installed.

Benchmarking
~~~~~~~~~~~~

Follow the directions for simple build except run cmake with the
``ARROW_BUILD_BENCHMARKS`` parameter set to ``ON``:

.. code-block:: shell

    cmake -DARROW_BUILD_TESTS=ON -DARROW_BUILD_BENCHMARKS=ON ..

and instead of make unittest run either ``make; ctest`` to run both unit tests
and benchmarks or ``make benchmark`` to run only the benchmarks. Benchmark logs
will be placed in the build directory under ``build/benchmark-logs``.

You can also invoke a single benchmark executable directly:

.. code-block:: shell

   ./release/arrow-builder-benchmark

The build system uses ``CMAKE_BUILD_TYPE=release`` by default which enables
compiler optimizations. It is also recommended to disable CPU throttling or
such hardware features as "Turbo Boost" to obtain more consistent and
comparable. benchmark results

Testing with LLVM AddressSanitizer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To use AddressSanitizer (ASAN) to find bad memory accesses or leaks with LLVM,
pass ``-DARROW_USE_ASAN=ON`` when building. You must use clang to compile with
ASAN, and ``ARROW_USE_ASAN`` is mutually-exclusive with the valgrind option
``ARROW_TEST_MEMCHECK``.

Fuzz testing with libfuzzer
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Fuzzers can help finding unhandled exceptions and problems with untrusted input
that may lead to crashes, security issues and undefined behavior. They do this
by generating random input data and observing the behavior of the executed
code. To build the fuzzer code, LLVM is required (GCC-based compilers won't
work). You can build them using the following code:

.. code-block:: shell

   cmake -DARROW_FUZZING=ON -DARROW_USE_ASAN=ON ..

``ARROW_FUZZING`` will enable building of fuzzer executables as well as enable the
addition of coverage helpers via ``ARROW_USE_COVERAGE``, so that the fuzzer can observe
the program execution.

It is also wise to enable some sanitizers like ``ARROW_USE_ASAN`` (see above), which
activates the address sanitizer. This way, we ensure that bad memory operations
provoked by the fuzzer will be found early. You may also enable other sanitizers as
well. Just keep in mind that some of them do not work together and some may result
in very long execution times, which will slow down the fuzzing procedure.

Now you can start one of the fuzzer, e.g.:

.. code-block:: shell

   ./debug/debug/ipc-fuzzing-test

This will try to find a malformed input that crashes the payload and will show the
stack trace as well as the input data. After a problem was found this way, it should
be reported and fixed. Usually, the fuzzing process cannot be continued until the
fix is applied, since the fuzzer usually converts to the problem again.

If you build fuzzers with ASAN, you need to set the ``ASAN_SYMBOLIZER_PATH``
environment variable to the absolute path of ``llvm-symbolizer``, which is a tool
that ships with LLVM.

.. code-block:: shell

   export ASAN_SYMBOLIZER_PATH=$(type -p llvm-symbolizer)

Note that some fuzzer builds currently reject paths with a version qualifier
(like ``llvm-sanitizer-5.0``). To overcome this, set an appropriate symlink
(here, when using LLVM 5.0):

.. code-block:: shell

   ln -sf /usr/bin/llvm-sanitizer-5.0 /usr/bin/llvm-sanitizer

There are some problems that may occur during the compilation process:

- libfuzzer was not distributed with your LLVM: ``ld: file not found: .../libLLVMFuzzer.a``
- your LLVM is too old: ``clang: error: unsupported argument 'fuzzer' to option 'fsanitize='``

Extra debugging help
~~~~~~~~~~~~~~~~~~~~

If you use the CMake option ``-DARROW_EXTRA_ERROR_CONTEXT=ON`` it will compile
the libraries with extra debugging information on error checks inside the
``RETURN_NOT_OK`` macro. In unit tests with ``ASSERT_OK``, this will yield error
outputs like:

.. code-block:: shell

   ../src/arrow/ipc/ipc-read-write-test.cc:609: Failure
   Failed
   ../src/arrow/ipc/metadata-internal.cc:508 code: TypeToFlatbuffer(fbb, *field.type(), &children, &layout, &type_enum, dictionary_memo, &type_offset)
   ../src/arrow/ipc/metadata-internal.cc:598 code: FieldToFlatbuffer(fbb, *schema.field(i), dictionary_memo, &offset)
   ../src/arrow/ipc/metadata-internal.cc:651 code: SchemaToFlatbuffer(fbb, schema, dictionary_memo, &fb_schema)
   ../src/arrow/ipc/writer.cc:697 code: WriteSchemaMessage(schema_, dictionary_memo_, &schema_fb)
   ../src/arrow/ipc/writer.cc:730 code: WriteSchema()
   ../src/arrow/ipc/writer.cc:755 code: schema_writer.Write(&dictionaries_)
   ../src/arrow/ipc/writer.cc:778 code: CheckStarted()
   ../src/arrow/ipc/ipc-read-write-test.cc:574 code: writer->WriteRecordBatch(batch)
   NotImplemented: Unable to convert type: decimal(19, 4)

Deprecations and API Changes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We use the compiler definition ``ARROW_NO_DEPRECATED_API`` to disable APIs that
have been deprecated. It is a good practice to compile third party applications
with this flag to proactively catch and account for API changes.

Cleaning includes with include-what-you-use (IWYU)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We occasionally use Google's `include-what-you-use
<https://github.com/include-what-you-use/include-what-you-use>`_ tool, also
known as IWYU, to remove unnecessary imports. Since setting up IWYU can be a
bit tedious, we provide a ``docker-compose`` target for running it on the C++
codebase:

.. code-block:: shell

   make -f Makefile.docker build-iwyu
   docker-compose run lint

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
generated, you can build a comparision report using

.. code-block:: shell

   abi-compliance-checker -l libarrow -d1 ABI-PY-9.dump -d2 ABI-PY-10.dump

The report is then generated in ``compat_reports/libarrow`` as a HTML.

Developing on Windows
=====================

Like Linux and macOS, we have worked to enable builds to work "out of the box"
with CMake for a reasonably large subset of the project.

System Setup
~~~~~~~~~~~~

Microsoft provides the free Visual Studio Community edition. When doing
development in the the shell, you must initialize the development
environment.

For Visual Studio 2015, execute the following batch script:

.. code-block:: shell

   "C:\Program Files (x86)\Microsoft Visual Studio 14.0\VC\vcvarsall.bat" amd64

For Visual Studio 2017, the script is:

.. code-block:: shell

   "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64

One can configure a console emulator like `cmder <https://cmder.net/>`_ to
automatically launch this when starting a new development console.

Using conda-forge for build dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

`Miniconda <https://conda.io/miniconda.html>`_ is a minimal Python distribution
including the `conda <https://conda.io>`_ package manager. Some memers of the
Apache Arrow community participate in the maintenance of `conda-forge
<https://conda-forge.org/>`_, a community-maintained cross-platform package
repository for conda.

To use ``conda-forge`` for your C++ build dependencies on Windows, first
download and install a 64-bit distribution from the `Miniconda homepage
<https://conda.io/miniconda.html>`_

To configure ``conda`` to use the ``conda-forge`` channel by default, launch a
command prompt (``cmd.exe``) and run the command:

.. code-block:: shell

   conda config --add channels conda-forge

Now, you can bootstrap a build environment (call from the root directory of the
Arrow codebase):

.. code-block:: shell

   conda create -y -n arrow-dev --file=ci\conda_env_cpp.yml

Then "activate" this conda environment with:

.. code-block:: shell

   activate arrow-dev

If the environment has been activated, the Arrow build system will
automatically see the ``%CONDA_PREFIX%`` environment variable and use that for
resolving the build dependencies. This is equivalent to setting

.. code-block:: shell

   -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
   -DARROW_PACKAGE_PREFIX=%CONDA_PREFIX%\Library

Note that these packages are only supported for release builds. If you intend
to use ``-DCMAKE_BUILD_TYPE=debug`` then you must build the packages from
source.

.. note::

   If you run into any problems using conda packages for dependencies, a very
   common problem is mixing packages from the ``defaults`` channel with those
   from ``conda-forge``. You can examine the installed packages in your
   environment (and their origin) with ``conda list``

Building using Visual Studio (MSVC) Solution Files
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Change working directory in ``cmd.exe`` to the root directory of Arrow and do
an out of source build by generating a MSVC solution:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake .. -G "Visual Studio 14 2015 Win64" ^
         -DARROW_BUILD_TESTS=ON ^
         -DGTest_SOURCE=BUNDLED
   cmake --build . --config Release

.. note::

   Currently building the unit tests does not work properly with googletest
   from conda-forge, so we must use the ``BUNDLED`` source for building that
   dependency

Building with Ninja and clcache
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The `Ninja <https://ninja-build.org/>`_ build system offsets better build
parallelization, and the optional `clcache
<https://github.com/frerich/clcache/>`_ compiler cache which keeps track of
past compilations to avoid running them over and over again (in a way similar
to the Unix-specific ``ccache``).

Activate your conda build environment to first install those utilities:

.. code-block:: shell

   activate arrow-dev
   conda install -c conda-forge ninja
   pip install git+https://github.com/frerich/clcache.git

Change working directory in ``cmd.exe`` to the root directory of Arrow and
do an out of source build by generating Ninja files:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake -G "Ninja" -DARROW_BUILD_TESTS=ON ^
         -DGTest_SOURCE=BUNDLED ..
   cmake --build . --config Release

Building with NMake
~~~~~~~~~~~~~~~~~~~

Change working directory in ``cmd.exe`` to the root directory of Arrow and
do an out of source build using `nmake`:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake -G "NMake Makefiles" ..
   cmake --build . --config Release
   nmake

Debug builds
~~~~~~~~~~~~

To build Debug version of Arrow you should have pre-installed a Debug version
of Boost. It's recommended to configure cmake build with the following
variables for Debug build:

* ``-DARROW_BOOST_USE_SHARED=OFF``: enables static linking with boost debug
  libs and simplifies run-time loading of 3rd parties
* ``-DBOOST_ROOT``: sets the root directory of boost libs. (Optional)
* ``-DBOOST_LIBRARYDIR``: sets the directory with boost lib files. (Optional)

The command line to build Arrow in Debug will look something like this:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake .. -G "Visual Studio 14 2015 Win64" ^
         -DARROW_BOOST_USE_SHARED=OFF ^
         -DCMAKE_BUILD_TYPE=Debug ^
         -DBOOST_ROOT=C:/local/boost_1_63_0  ^
         -DBOOST_LIBRARYDIR=C:/local/boost_1_63_0/lib64-msvc-14.0
   cmake --build . --config Debug

Windows dependency resolution issues
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Because Windows uses `.lib` files for both static and dynamic linking of
dependencies, the static library sometimes may be named something different
like ``%PACKAGE%_static.lib`` to distinguish itself. If you are statically
linking some dependencies, we provide some options

* ``-DBROTLI_MSVC_STATIC_LIB_SUFFIX=%BROTLI_SUFFIX%``
* ``-DSNAPPY_MSVC_STATIC_LIB_SUFFIX=%SNAPPY_SUFFIX%``
* ``-LZ4_MSVC_STATIC_LIB_SUFFIX=%LZ4_SUFFIX%``
* ``-ZSTD_MSVC_STATIC_LIB_SUFFIX=%ZSTD_SUFFIX%``

To get the latest build instructions, you can reference `ci/appveyor-built.bat
<https://github.com/apache/arrow/blob/master/ci/appveyor-cpp-build.bat>`_,
which is used by automated Appveyor builds.

Statically linking to Arrow on Windows
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Arrow headers on Windows static library builds (enabled by the CMake
option ``ARROW_BUILD_STATIC``) use the preprocessor macro ``ARROW_STATIC`` to
suppress dllimport/dllexport marking of symbols. Projects that statically link
against Arrow on Windows additionally need this definition. The Unix builds do
not use the macro.

Replicating Appveyor Builds
~~~~~~~~~~~~~~~~~~~~~~~~~~~

For people more familiar with linux development but need to replicate a failing
appveyor build, here are some rough notes from replicating the
``Static_Crt_Build`` (make unittest will probably still fail but many unit
tests can be made with there individual make targets).

1. Microsoft offers trial VMs for `Windows with Microsoft Visual Studio
   <https://developer.microsoft.com/en-us/windows/downloads/virtual-machines>`_.
   Download and install a version.
2. Run the VM and install CMake and Miniconda or Anaconda (these instructions
   assume Anaconda).
3. Download `pre-built Boost debug binaries
   <https://sourceforge.net/projects/boost/files/boost-binaries/>`_ and install
   it (run from command prompt opened by "Developer Command Prompt for MSVC
   2017"):

.. code-block:: shell

   cd $EXTRACT_BOOST_DIRECTORY
   .\bootstrap.bat
   @rem This is for static libraries needed for static_crt_build in appvyor
   .\b2 link=static -with-filesystem -with-regex -with-system install
   @rem this should put libraries and headers in c:\Boost

4. Activate ananaconda/miniconda:

.. code-block:: shell

  @rem this might differ for miniconda
  C:\Users\User\Anaconda3\Scripts\activate

5. Clone and change directories to the arrow source code (you might need to
   install git).
6. Setup environment variables:

.. code-block:: shell

   @rem Change the build type based on which appveyor job you want.
   SET JOB=Static_Crt_Build
   SET GENERATOR=Ninja
   SET APPVEYOR_BUILD_WORKER_IMAGE=Visual Studio 2017
   SET USE_CLCACHE=false
   SET ARROW_BUILD_GANDIVA=OFF
   SET ARROW_LLVM_VERSION=7.0.*
   SET PYTHON=3.6
   SET ARCH=64
   SET PATH=C:\Users\User\Anaconda3;C:\Users\User\Anaconda3\Scripts;C:\Users\User\Anaconda3\Library\bin;%PATH%
   SET BOOST_LIBRARYDIR=C:\Boost\lib
   SET BOOST_ROOT=C:\Boost

7. Run appveyor scripts:

.. code-block:: shell

   .\ci\appveyor-install.bat
   @rem this might fail but at this point most unit tests should be buildable by there individual targets
   @rem see next line for example.
   .\ci\appveyor-build.bat
   cmake --build . --config Release --target arrow-compute-hash-test

Apache Parquet Development
==========================

To build the C++ libraries for Apache Parquet, add the flag
``-DARROW_PARQUET=ON`` when invoking CMake. The Parquet libraries and unit tests
can be built with the ``parquet`` make target:

.. code-block:: shell

   make parquet

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

Development Conventions
=======================

This section provides some information about some of the abstractions and
development approaches we use to solve problems common to many parts of the C++
project.

Memory Pools
~~~~~~~~~~~~

We provide a default memory pool with ``arrow::default_memory_pool()``. As a
matter of convenience, some of the array builder classes have constructors
which use the default pool without explicitly passing it. You can disable these
constructors in your application (so that you are accounting properly for all
memory allocations) by defining ``ARROW_NO_DEFAULT_MEMORY_POOL``.

Header files
~~~~~~~~~~~~

We use the ``.h`` extension for C++ header files. Any header file name not
containing ``internal`` is considered to be a public header, and will be
automatically installed by the build.

Error Handling and Exceptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For error handling, we use ``arrow::Status`` values instead of throwing C++
exceptions. Since the Arrow C++ libraries are intended to be useful as a
component in larger C++ projects, using ``Status`` objects can help with good
code hygiene by making explicit when a function is expected to be able to fail.

For expressing invariants and "cannot fail" errors, we use DCHECK macros
defined in ``arrow/util/logging.h``. These checks are disabled in release builds
and are intended to catch internal development errors, particularly when
refactoring. These macros are not to be included in any public header files.

Since we do not use exceptions, we avoid doing expensive work in object
constructors. Objects that are expensive to construct may often have private
constructors, with public static factory methods that return ``Status``.

There are a number of object constructors, like ``arrow::Schema`` and
``arrow::RecordBatch`` where larger STL container objects like ``std::vector`` may
be created. While it is possible for ``std::bad_alloc`` to be thrown in these
constructors, the circumstances where they would are somewhat esoteric, and it
is likely that an application would have encountered other more serious
problems prior to having ``std::bad_alloc`` thrown in a constructor.
