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

.. _developers-cpp-emscripten:
===============================================
Cross compiling for WebAssembly with Emscripten
===============================================

Prerequisites
-------------
You need CMake and compilers etc. installed as per the normal build instructions. Before building with Emscripten, you also need to install Emscripten and
activate it using the commands below (see https://emscripten.org/docs/getting_started/downloads.html for details).

.. code:: shell

   git clone https://github.com/emscripten-core/emsdk.git
   cd emsdk
   # replace <version> with the desired EMSDK version.
   # e.g. for Pyodide 0.24, you need EMSDK version 3.1.45
   ./emsdk install <version>
   ./emsdk activate <version>
   source ./emsdk_env.sh

If you want to build PyArrow for `Pyodide <https://pyodide.org>`_, you
need ``pyodide-build`` installed via ``pip``, and to be running with the
same version of Python that Pyodide is built for, along with the same
versions of emsdk tools.

.. code:: shell

   # install Pyodide build tools.
   # e.g. for version 0.24 of Pyodide:
   pip install pyodide-build==0.24

Then build with the ``ninja-release-emscripten`` CMake preset,
like below:

.. code:: shell

   emcmake cmake --preset "ninja-release-emscripten"
   ninja install

This will install a built static library version of ``libarrow`` it into the
Emscripten sysroot cache, meaning you can build things that depend on it
and they will find ``libarrow``.

e.g. if you want to build for Pyodide, run the commands above, and then
go to ``arrow/python`` and run

.. code:: shell

   pyodide build

It should make a wheel targeting the currently enabled version of
Pyodide (i.e. the version corresponding to the currently installed
``pyodide-build``) in the ``dist`` subdirectory.


Manual Build
------------

If you want to manually build for Emscripten, take a look at the
``CMakePresets.json`` file in the ``arrow/cpp`` directory for a list of things
you will need to override. In particular you will need:

#. Build dependencies set to ``BUNDLED``, so it uses properly cross
   compiled build dependencies.

#. ``CMAKE_TOOLCHAIN_FILE`` set by using ``emcmake cmake`` instead of just ``cmake``.

#. You will quite likely need to set ``ARROW_ENABLE_THREADING`` to ``OFF``
   for builds targeting single threaded Emscripten environments such as
   Pyodide.

#. ``ARROW_FLIGHT`` and anything else that uses network probably won't
   work.

#. ``ARROW_JEMALLOC`` and ``ARROW_MIMALLOC`` again probably need to be
   ``OFF``

#. ``ARROW_BUILD_STATIC`` set to ``ON`` and ``ARROW_BUILD_SHARED`` set to
   ``OFF`` is most likely to work.
