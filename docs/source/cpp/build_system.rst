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

===================================
Using Arrow C++ in your own project
===================================

This section assumes you already have the Arrow C++ libraries on your
system, either after installing them using a package manager or after
:ref:`building them yourself <building-arrow-cpp>`.

The recommended way to integrate the Arrow C++ libraries in your own
C++ project is to use CMake's `find_package
<https://cmake.org/cmake/help/latest/command/find_package.html>`_
function for locating and integrating dependencies. If you don't use
CMake as a build system, you can use `pkg-config
<https://www.freedesktop.org/wiki/Software/pkg-config/>`_ to find
installed the Arrow C++ libraries.

CMake
=====

Basic usage
-----------

This minimal ``CMakeLists.txt`` file compiles a ``my_example.cc`` source
file into an executable linked with the Arrow C++ shared library:

.. code-block:: cmake

   project(MyExample)

   find_package(Arrow REQUIRED)

   add_executable(my_example my_example.cc)
   target_link_libraries(my_example PRIVATE arrow_shared)

Available variables and targets
-------------------------------

The directive ``find_package(Arrow REQUIRED)`` asks CMake to find an Arrow
C++ installation on your system.  When it returns, it will have set a few
CMake variables:

* ``${Arrow_FOUND}`` is true if the Arrow C++ libraries have been found
* ``${ARROW_VERSION}`` contains the Arrow version string
* ``${ARROW_FULL_SO_VERSION}`` contains the Arrow DLL version string

In addition, it will have created some targets that you can link against
(note these are plain strings, not variables):

* ``arrow_shared`` links to the Arrow shared libraries
* ``arrow_static`` links to the Arrow static libraries

In most cases, it is recommended to use the Arrow shared libraries.

.. note::
   CMake is case-sensitive.  The names and variables listed above have to be
   spelt exactly that way!

.. seealso::
   A Docker-based :doc:`minimal build example <examples/cmake_minimal_build>`.

pkg-config
==========

Basic usage
-----------

You can get suitable build flags by the following command line:

.. code-block:: shell

   pkg-config --cflags --libs arrow

If you want to link the Arrow C++ static library, you need to add
``--static`` option:

.. code-block:: shell

   pkg-config --cflags --libs --static arrow

This minimal ``Makefile`` file compiles a ``my_example.cc`` source
file into an executable linked with the Arrow C++ shared library:

.. code-block:: makefile

   my_example: my_example.cc
   	$(CXX) -o $@ $(CXXFLAGS) $< $$(pkg-config --cflags --libs arrow)

Many build systems support pkg-config. For example:

  * `GNU Autotools <https://people.freedesktop.org/~dbn/pkg-config-guide.html#using>`_
  * `CMake <https://cmake.org/cmake/help/latest/module/FindPkgConfig.html>`_
    (But you should use ``find_package(Arrow)`` instead.)
  * `Meson <https://mesonbuild.com/Reference-manual.html#dependency>`_

Available packages
------------------

The Arrow C++ provides a pkg-config package for each module. Here are
all available packages:

  * ``arrow-csv``
  * ``arrow-cuda``
  * ``arrow-dataset``
  * ``arrow-filesystem``
  * ``arrow-flight-testing``
  * ``arrow-flight``
  * ``arrow-json``
  * ``arrow-orc``
  * ``arrow-python-flight``
  * ``arrow-python``
  * ``arrow-tensorflow``
  * ``arrow-testing``
  * ``arrow``
  * ``gandiva``
  * ``parquet``
  * ``plasma``

A Note on Linking
=================

Some Arrow components have dependencies that you may want to use in your own
project. Care must be taken to ensure that your project links the same version
of these dependencies in the same way (statically or dynamically) as Arrow,
else `ODR <https://en.wikipedia.org/wiki/One_Definition_Rule>`_ violations may
result and your program may crash or silently corrupt data.

In particular, Arrow Flight and its dependencies `Protocol Buffers (Protobuf)
<https://developers.google.com/protocol-buffers/>`_ and `gRPC
<https://grpc.io/>`_ are likely to cause issues. When using Arrow Flight, note
the following guidelines:

* If statically linking Arrow Flight, Protobuf and gRPC must also be statically
  linked, and the same goes for dynamic linking.
* Some platforms (e.g. Ubuntu 20.04 at the time of this writing) may ship a
  version of Protobuf and/or gRPC that is not recent enough for Arrow
  Flight. In that case, Arrow Flight bundles these dependencies, so care must
  be taken not to mix the Arrow Flight library with the platform Protobuf/gRPC
  libraries (as then you will have two versions of Protobuf and/or gRPC linked
  into your application).

It may be easiest to depend on a version of Arrow built from source, where you
can control the source of each dependency and whether it is statically or
dynamically linked. See :doc:`/developers/cpp/building` for instructions. Or
alternatively, use Arrow from a package manager such as Conda or vcpkg which
will manage consistent versions of Arrow and its dependencies.


.. _download-timezone-database:

Runtime Dependencies
====================

While Arrow uses the OS-provided timezone database on Linux and macOS, it
requires a user-provided database on Windows. You must download and extract the
text version of the IANA timezone database and add the Windows timezone mapping
XML. To download, you can use the following batch script:

.. literalinclude:: ../../../ci/appveyor-cpp-setup.bat
   :language: batch
   :start-after: @rem (Doc section: Download timezone database)
   :end-before: @rem (Doc section: Download timezone database)

By default, the timezone database will be detected at ``%USERPROFILE%\Downloads\tzdata``,
but you can set a custom path at runtime in :struct:`arrow::ArrowGlobalOptions`::

   arrow::ArrowGlobalOptions options;
   options.tz_db_path = "path/to/tzdata";
   ARROW_RETURN_NOT_OK(arrow::Initialize(options));
