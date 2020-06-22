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

Using Arrow C++ in your own project
===================================

This section assumes you already have the Arrow C++ libraries on your
system, either after installing them using a package manager or after
:ref:`building them yourself <building-arrow-cpp>`.

The recommended way to integrate the Arrow C++ libraries in your own C++
project is to use CMake's
`find_package <https://cmake.org/cmake/help/latest/command/find_package.html>`_
function for locating and integrating dependencies.

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
