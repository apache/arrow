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

.. _building-arrow-cpp:

==================
Building Arrow C++
==================

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

* A C++17-enabled compiler. On Linux, gcc 7.1 and higher should be
  sufficient. For Windows, at least Visual Studio VS2017 is required.
* CMake 3.5 or higher
* On Linux and macOS, either ``make`` or ``ninja`` build utilities
* At least 1GB of RAM for a minimal build, 4GB for a minimal  
  debug build with tests and 8GB for a full build using
  :ref:`docker <docker-builds>`.

On Ubuntu/Debian you can install the requirements with:

.. code-block:: shell

   sudo apt-get install \
        build-essential \
        ninja-build \
        cmake

On Alpine Linux:

.. code-block:: shell

   apk add autoconf \
           bash \
           cmake \
           g++ \
           gcc \
           ninja \
           make
           
On Fedora Linux:

.. code-block:: shell

   sudo dnf install \
        cmake \
        gcc \
        gcc-c++ \
        ninja-build \
        make

On Arch Linux:

.. code-block:: shell

   sudo pacman -S --needed \
        base-devel \
        ninja \
        cmake

On macOS, you can use `Homebrew <https://brew.sh/>`_:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git
   cd arrow
   brew update && brew bundle --file=cpp/Brewfile

With `vcpkg <https://github.com/Microsoft/vcpkg>`_:

.. code-block:: shell
   
   git clone https://github.com/apache/arrow.git
   cd arrow
   vcpkg install \
     --x-manifest-root cpp \
     --feature-flags=versions \
     --clean-after-build

On MSYS2:

.. code-block:: shell

   pacman --sync --refresh --noconfirm \
     ccache \
     git \
     mingw-w64-${MSYSTEM_CARCH}-boost \
     mingw-w64-${MSYSTEM_CARCH}-brotli \
     mingw-w64-${MSYSTEM_CARCH}-cmake \
     mingw-w64-${MSYSTEM_CARCH}-gcc \
     mingw-w64-${MSYSTEM_CARCH}-gflags \
     mingw-w64-${MSYSTEM_CARCH}-glog \
     mingw-w64-${MSYSTEM_CARCH}-gtest \
     mingw-w64-${MSYSTEM_CARCH}-lz4 \
     mingw-w64-${MSYSTEM_CARCH}-protobuf \
     mingw-w64-${MSYSTEM_CARCH}-python3-numpy \
     mingw-w64-${MSYSTEM_CARCH}-rapidjson \
     mingw-w64-${MSYSTEM_CARCH}-snappy \
     mingw-w64-${MSYSTEM_CARCH}-thrift \
     mingw-w64-${MSYSTEM_CARCH}-zlib \
     mingw-w64-${MSYSTEM_CARCH}-zstd

.. _cpp-building-building:

Building
========

All the instructions below assume that you have cloned the Arrow git
repository and navigated to the ``cpp`` subdirectory:

.. code-block::

   $ git clone https://github.com/apache/arrow.git
   $ cd arrow/cpp

.. _cmake_presets:

CMake presets
-------------

Using CMake version 3.21.0 or higher, some presets for various build
configurations are provided.  You can get a list of the available presets
using ``cmake --list-presets``:

.. code-block::

   $ cmake --list-presets   # from inside the `cpp` subdirectory
   Available configure presets:

     "ninja-debug-minimal"     - Debug build without anything enabled
     "ninja-debug-basic"       - Debug build with tests and reduced dependencies
     "ninja-debug"             - Debug build with tests and more optional components
      [ etc. ]

You can inspect the specific options enabled by a given preset using
``cmake -N --preset <preset name>``:

.. code-block::

   $ cmake --preset -N ninja-debug-minimal
   Preset CMake variables:

     ARROW_BUILD_INTEGRATION="OFF"
     ARROW_BUILD_STATIC="OFF"
     ARROW_BUILD_TESTS="OFF"
     ARROW_EXTRA_ERROR_CONTEXT="ON"
     ARROW_WITH_RE2="OFF"
     ARROW_WITH_UTF8PROC="OFF"
     CMAKE_BUILD_TYPE="Debug"

You can also create a build from a given preset:

.. code-block::

   $ mkdir build   # from inside the `cpp` subdirectory
   $ cd build
   $ cmake .. --preset ninja-debug-minimal
      Preset CMake variables:

        ARROW_BUILD_INTEGRATION="OFF"
        ARROW_BUILD_STATIC="OFF"
        ARROW_BUILD_TESTS="OFF"
        ARROW_EXTRA_ERROR_CONTEXT="ON"
        ARROW_WITH_RE2="OFF"
        ARROW_WITH_UTF8PROC="OFF"
        CMAKE_BUILD_TYPE="Debug"

      -- Building using CMake version: 3.21.3
      [ etc. ]

and then ask to compile the build targets:

.. code-block::

   $ cmake --build .
   [142/142] Creating library symlink debug/libarrow.so.700 debug/libarrow.so

   $ tree debug/
   debug/
   ├── libarrow.so -> libarrow.so.700
   ├── libarrow.so.700 -> libarrow.so.700.0.0
   └── libarrow.so.700.0.0

   0 directories, 3 files

When creating a build, it is possible to pass custom options besides
the preset-defined ones, for example:

.. code-block::

   $ cmake .. --preset ninja-debug-minimal -DCMAKE_INSTALL_PREFIX=/usr/local

.. note::
   The CMake presets are provided as a help to get started with Arrow
   development and understand common build configurations.  They are not
   guaranteed to be immutable but may change in the future based on
   feedback.

   Instead of relying on CMake presets, it is **highly recommended** that
   automated builds, continuous integration, release scripts, etc. use
   manual configuration, as outlined below.

.. seealso::
   `Official documentation for CMake presets <https://cmake.org/cmake/help/v3.21/manual/cmake-presets.7.html>`_.


Manual configuration
--------------------

The build system uses ``CMAKE_BUILD_TYPE=release`` by default, so if this
argument is omitted then a release build will be produced.

.. note::

   You need to set more options to build on Windows. See
   :ref:`developers-cpp-windows` for details.

Several build types are possible:

* ``Debug``: doesn't apply any compiler optimizations and adds debugging
  information in the binary.
* ``RelWithDebInfo``: applies compiler optimizations while adding debug
  information in the binary.
* ``Release``: applies compiler optimizations and removes debug information
  from the binary.

You can also run default build with flag ``-DARROW_EXTRA_ERROR_CONTEXT=ON``, see
:ref:`cpp-extra-debugging`.

Minimal release build (1GB of RAM for building or more recommended):

.. code-block::

   $ mkdir build-release
   $ cd build-release
   $ cmake ..
   $ make -j8       # if you have 8 CPU cores, otherwise adjust

Minimal debug build with unit tests (4GB of RAM for building or more recommended):

.. code-block::

   $ git submodule update --init --recursive
   $ export ARROW_TEST_DATA=$PWD/../testing/data
   $ mkdir build-debug
   $ cd build-debug
   $ cmake -DCMAKE_BUILD_TYPE=Debug -DARROW_BUILD_TESTS=ON ..
   $ make -j8       # if you have 8 CPU cores, otherwise adjust
   $ make unittest  # to run the tests

The unit tests are not built by default. After building, one can also invoke
the unit tests using the ``ctest`` tool provided by CMake (note that ``test``
depends on ``python`` being available).

On some Linux distributions, running the test suite might require setting an
explicit locale. If you see any locale-related errors, try setting the
environment variable (which requires the `locales` package or equivalent):

.. code-block::

   $ export LC_ALL="en_US.UTF-8"

Faster builds with Ninja
~~~~~~~~~~~~~~~~~~~~~~~~

Many contributors use the `Ninja build system <https://ninja-build.org/>`_ to
get faster builds. It especially speeds up incremental builds. To use
``ninja``, pass ``-GNinja`` when calling ``cmake`` and then use the ``ninja``
command instead of ``make``.

Unity builds
~~~~~~~~~~~~

The CMake
`unity builds <https://cmake.org/cmake/help/latest/prop_tgt/UNITY_BUILD.html>`_
option can make full builds significantly faster, but it also increases the
memory requirements.  Consider turning it on (using ``-DCMAKE_UNITY_BUILD=ON``)
if memory consumption is not an issue.

.. _cpp_build_optional_components:

Optional Components
~~~~~~~~~~~~~~~~~~~

By default, the C++ build system creates a fairly minimal build. We have
several optional system components which you can opt into building by passing
boolean flags to ``cmake``.

* ``-DARROW_BUILD_UTILITIES=ON`` : Build Arrow commandline utilities
* ``-DARROW_COMPUTE=ON``: Computational kernel functions and other support
* ``-DARROW_CSV=ON``: CSV reader module
* ``-DARROW_CUDA=ON``: CUDA integration for GPU development. Depends on NVIDIA
  CUDA toolkit. The CUDA toolchain used to build the library can be customized
  by using the ``$CUDA_HOME`` environment variable.
* ``-DARROW_DATASET=ON``: Dataset API, implies the Filesystem API
* ``-DARROW_FILESYSTEM=ON``: Filesystem API for accessing local and remote
  filesystems
* ``-DARROW_FLIGHT=ON``: Arrow Flight RPC system, which depends at least on
  gRPC
* ``-DARROW_FLIGHT_SQL=ON``: Arrow Flight SQL
* ``-DARROW_GANDIVA=ON``: Gandiva expression compiler, depends on LLVM,
  Protocol Buffers, and re2
* ``-DARROW_GANDIVA_JAVA=ON``: Gandiva JNI bindings for Java
* ``-DARROW_GCS=ON``: Build Arrow with GCS support (requires the GCloud SDK for C++)
* ``-DARROW_HDFS=ON``: Arrow integration with libhdfs for accessing the Hadoop
  Filesystem
* ``-DARROW_JEMALLOC=ON``: Build the Arrow jemalloc-based allocator, on by default 
* ``-DARROW_JSON=ON``: JSON reader module
* ``-DARROW_MIMALLOC=ON``: Build the Arrow mimalloc-based allocator
* ``-DARROW_ORC=ON``: Arrow integration with Apache ORC
* ``-DARROW_PARQUET=ON``: Apache Parquet libraries and Arrow integration
* ``-DPARQUET_REQUIRE_ENCRYPTION=ON``: Parquet Modular Encryption
* ``-DARROW_PLASMA=ON``: Plasma Shared Memory Object Store
* ``-DARROW_PLASMA_JAVA_CLIENT=ON``: Build Java client for Plasma
* ``-DARROW_PYTHON=ON``: This option is deprecated since 10.0.0. This
  will be removed in a future release. Use CMake presets instead. Or
  you can enable ``ARROW_COMPUTE``, ``ARROW_CSV``, ``ARROW_DATASET``,
  ``ARROW_FILESYSTEM``, ``ARROW_HDFS``, and ``ARROW_JSON`` directly
  instead.
* ``-DARROW_S3=ON``: Support for Amazon S3-compatible filesystems
* ``-DARROW_WITH_RE2=ON`` Build with support for regular expressions using the re2 
  library, on by default and used when ``ARROW_COMPUTE`` or ``ARROW_GANDIVA`` is ``ON``
* ``-DARROW_WITH_UTF8PROC=ON``: Build with support for Unicode properties using
  the utf8proc library, on by default and used when ``ARROW_COMPUTE`` or ``ARROW_GANDIVA``
  is ``ON``
* ``-DARROW_TENSORFLOW=ON``: Build Arrow with TensorFlow support enabled

Compression options available in Arrow are:

* ``-DARROW_WITH_BROTLI=ON``: Build support for Brotli compression
* ``-DARROW_WITH_BZ2=ON``: Build support for BZ2 compression
* ``-DARROW_WITH_LZ4=ON``: Build support for lz4 compression
* ``-DARROW_WITH_SNAPPY=ON``: Build support for Snappy compression
* ``-DARROW_WITH_ZLIB=ON``: Build support for zlib (gzip) compression
* ``-DARROW_WITH_ZSTD=ON``: Build support for ZSTD compression

Some features of the core Arrow shared library can be switched off for improved
build times if they are not required for your application:

* ``-DARROW_IPC=ON``: build the IPC extensions

.. warning::
   Plasma is deprecated as of Arrow 10.0.0, and will be removed in 12.0.0 or so.

Optional Targets
~~~~~~~~~~~~~~~~

For development builds, you will often want to enable additional targets in
enable to exercise your changes, using the following ``cmake`` options.

* ``-DARROW_BUILD_BENCHMARKS=ON``: Build executable benchmarks.
* ``-DARROW_BUILD_EXAMPLES=ON``: Build examples of using the Arrow C++ API.
* ``-DARROW_BUILD_INTEGRATION=ON``: Build additional executables that are
  used to exercise protocol interoperability between the different Arrow
  implementations.
* ``-DARROW_BUILD_UTILITIES=ON``: Build executable utilities.
* ``-DARROW_BUILD_TESTS=ON``: Build executable unit tests.
* ``-DARROW_ENABLE_TIMING_TESTS=ON``: If building unit tests, enable those
  unit tests that rely on wall-clock timing (this flag is disabled on CI
  because it can make test results flaky).
* ``-DARROW_FUZZING=ON``: Build fuzz targets and related executables.

Optional Checks
~~~~~~~~~~~~~~~

The following special checks are available as well.  They instrument the
generated code in various ways so as to detect select classes of problems
at runtime (for example when executing unit tests).

* ``-DARROW_USE_ASAN=ON``: Enable Address Sanitizer to check for memory leaks,
  buffer overflows or other kinds of memory management issues.
* ``-DARROW_USE_TSAN=ON``: Enable Thread Sanitizer to check for races in
  multi-threaded code.
* ``-DARROW_USE_UBSAN=ON``: Enable Undefined Behavior Sanitizer to check for
  situations which trigger C++ undefined behavior.

Some of those options are mutually incompatible, so you may have to build
several times with different options if you want to exercise all of them.

CMake version requirements
~~~~~~~~~~~~~~~~~~~~~~~~~~

While we support CMake 3.5 and higher, some features require a newer version of
CMake:

* Building the benchmarks requires 3.6 or higher
* Building zstd from source requires 3.7 or higher
* Building Gandiva JNI bindings requires 3.11 or higher

LLVM and Clang Tools
~~~~~~~~~~~~~~~~~~~~

We are currently using LLVM for library builds and for other developer tools
such as code formatting with ``clang-format``. LLVM can be installed via most
modern package managers (apt, yum, conda, Homebrew, vcpkg, chocolatey).

.. _cpp-build-dependency-management:

Build Dependency Management
===========================

The build system supports a number of third-party dependencies

  * ``AWSSDK``: for S3 support, requires system cURL and can use the
    ``BUNDLED`` method described below
  * ``benchmark``: Google benchmark, for testing
  * ``Boost``: for cross-platform support
  * ``Brotli``: for data compression
  * ``BZip2``: for data compression
  * ``c-ares``: a dependency of gRPC
  * ``gflags``: for command line utilities (formerly Googleflags)
  * ``GLOG``: for logging
  * ``google_cloud_cpp_storage``: for Google Cloud Storage support, requires 
    system cURL and can use the ``BUNDLED`` method described below
  * ``gRPC``: for remote procedure calls
  * ``GTest``: Googletest, for testing
  * ``LLVM``: a dependency of Gandiva
  * ``Lz4``: for data compression
  * ``ORC``: for Apache ORC format support
  * ``re2``: for compute kernels and Gandiva, a dependency of gRPC
  * ``Protobuf``: Google Protocol Buffers, for data serialization
  * ``RapidJSON``: for data serialization
  * ``Snappy``: for data compression
  * ``Thrift``: Apache Thrift, for data serialization
  * ``utf8proc``: for compute kernels
  * ``ZLIB``: for data compression
  * ``zstd``: for data compression

The CMake option ``ARROW_DEPENDENCY_SOURCE`` is a global option that instructs
the build system how to resolve each dependency. There are a few options:

* ``AUTO``: Try to find package in the system default locations and build from
  source if not found
* ``BUNDLED``: Building the dependency automatically from source
* ``SYSTEM``: Finding the dependency in system paths using CMake's built-in
  ``find_package`` function, or using ``pkg-config`` for packages that do not
  have this feature
* ``CONDA``: Use ``$CONDA_PREFIX`` as alternative ``SYSTEM`` PATH
* ``VCPKG``: Find dependencies installed by vcpkg, and if not found, run
  ``vcpkg install`` to install them
* ``BREW``: Use Homebrew default paths as an alternative ``SYSTEM`` path

The default method is ``AUTO`` unless you are developing within an active conda
environment (detected by presence of the ``$CONDA_PREFIX`` environment
variable), in which case it is ``CONDA``.

Individual Dependency Resolution
--------------------------------

While ``-DARROW_DEPENDENCY_SOURCE=$SOURCE`` sets a global default for all
packages, the resolution strategy can be overridden for individual packages by
setting ``-D$PACKAGE_NAME_SOURCE=..``. For example, to build Protocol Buffers
from source, set

.. code-block:: shell

   -DProtobuf_SOURCE=BUNDLED

This variable is unfortunately case-sensitive; the name used for each package
is listed above, but the most up-to-date listing can be found in
`cpp/cmake_modules/ThirdpartyToolchain.cmake <https://github.com/apache/arrow/blob/main/cpp/cmake_modules/ThirdpartyToolchain.cmake>`_.

Bundled Dependency Versions
---------------------------

When using the ``BUNDLED`` method to build a dependency from source, the
version number from ``cpp/thirdparty/versions.txt`` is used. There is also a
dependency source downloader script (see below), which can be used to set up
offline builds.

When using ``BUNDLED`` for dependency resolution (and if you use either the
jemalloc or mimalloc allocators, which are recommended), statically linking the
Arrow libraries in a third party project is more complex. See below for
instructions about how to configure your build system in this case.

Boost-related Options
---------------------

We depend on some Boost C++ libraries for cross-platform support. In most cases,
the Boost version available in your package manager may be new enough, and the
build system will find it automatically. If you have Boost installed in a
non-standard location, you can specify it by passing
``-DBOOST_ROOT=$MY_BOOST_ROOT`` or setting the ``BOOST_ROOT`` environment
variable.

Offline Builds
--------------

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

.. code-block::

   # Download tarballs into $HOME/arrow-thirdparty
   $ ./thirdparty/download_dependencies.sh $HOME/arrow-thirdparty

You can then invoke CMake to create the build directory and it will use the
declared environment variable pointing to downloaded archives instead of
downloading them (one for each build dir!).

Statically Linking
------------------

When ``-DARROW_BUILD_STATIC=ON``, all build dependencies built as static
libraries by the Arrow build system will be merged together to create a static
library ``arrow_bundled_dependencies``. In UNIX-like environments (Linux, macOS,
MinGW), this is called ``libarrow_bundled_dependencies.a`` and on Windows with
Visual Studio ``arrow_bundled_dependencies.lib``. This "dependency bundle"
library is installed in the same place as the other Arrow static libraries.

If you are using CMake, the bundled dependencies will automatically be included
when linking if you use the ``arrow_static`` CMake target. In other build
systems, you may need to explicitly link to the dependency bundle. We created
an `example CMake-based build configuration
<https://github.com/apache/arrow/tree/main/cpp/examples/minimal_build>`_ to
show you a working example.

On Linux and macOS, if your application does not link to the ``pthread``
library already, you must include ``-pthread`` in your linker setup. In CMake
this can be accomplished with the ``Threads`` built-in package:

.. code-block:: cmake

   set(THREADS_PREFER_PTHREAD_FLAG ON)
   find_package(Threads REQUIRED)
   target_link_libraries(my_target PRIVATE Threads::Threads)

.. _cpp-extra-debugging:

Extra debugging help
--------------------

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
----------------------------

We use the compiler definition ``ARROW_NO_DEPRECATED_API`` to disable APIs that
have been deprecated. It is a good practice to compile third party applications
with this flag to proactively catch and account for API changes.

Modular Build Targets
---------------------

Since there are several major parts of the C++ project, we have provided
modular CMake targets for building each library component, group of unit tests
and benchmarks, and their dependencies:

* ``make arrow`` for Arrow core libraries
* ``make parquet`` for Parquet libraries
* ``make gandiva`` for Gandiva (LLVM expression compiler) libraries
* ``make plasma`` for Plasma libraries, server

.. note::
   If you have selected Ninja as CMake generator, replace ``make arrow`` with
   ``ninja arrow``, and so on.

To build the unit tests or benchmarks, add ``-tests`` or ``-benchmarks``
to the target name. So ``make arrow-tests`` will build the Arrow core unit
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

Debugging with Xcode on macOS
-----------------------------

Xcode is the IDE provided with macOS and can be use to develop and debug Arrow
by generating an Xcode project:

.. code-block:: shell

   cd cpp
   mkdir xcode-build
   cd xcode-build
   cmake .. -G Xcode -DARROW_BUILD_TESTS=ON -DCMAKE_BUILD_TYPE=DEBUG
   open arrow.xcodeproj

This will generate a project and open it in the Xcode app. As an alternative,
the command ``xcodebuild`` will perform a command-line build using the
generated project. It is recommended to use the "Automatically Create Schemes"
option when first launching the project.  Selecting an auto-generated scheme
will allow you to build and run a unittest with breakpoints enabled.
