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

.. _developers-cpp-windows:

=====================
Developing on Windows
=====================

Like Linux and macOS, we have worked to enable builds to work "out of the box"
with CMake for a reasonably large subset of the project.

System Setup
============

Microsoft provides the free Visual Studio Community edition. When doing
development in the shell, you must initialize the development
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
========================================

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
==================================================

Change working directory in ``cmd.exe`` to the root directory of Arrow and do
an out of source build by generating a MSVC solution:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake .. -G "Visual Studio 14 2015 Win64" ^
         -DARROW_BUILD_TESTS=ON
   cmake --build . --config Release

Building with Ninja and clcache
===============================

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
   cmake -G "Ninja" ^
         -DCMAKE_C_COMPILER=clcache ^
         -DCMAKE_CXX_COMPILER=clcache ^
         -DARROW_BUILD_TESTS=ON ^
         -DGTest_SOURCE=BUNDLED ..
   cmake --build . --config Release

Setting ``CMAKE_C_COMPILER`` and ``CMAKE_CXX_COMPILER`` in the command line 
of ``cmake`` is the preferred method of using ``clcache``. Alternatively, you 
can set ``CC`` and ``CXX`` environment variables before calling ``cmake``:

.. code-block:: shell

   ...
   set CC=clcache
   set CXX=clcache
   cmake -G "Ninja" ^
   ...



Building with NMake
===================

Change working directory in ``cmd.exe`` to the root directory of Arrow and
do an out of source build using ``nmake``:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake -G "NMake Makefiles" ..
   nmake

Building on MSYS2
=================

You can build on MSYS2 terminal, ``cmd.exe`` or PowerShell terminal.

On MSYS2 terminal:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake -G "MSYS Makefiles" ..
   make

On ``cmd.exe`` or PowerShell terminal, you can use the following batch
file:

.. code-block:: batch

   setlocal

   REM For 64bit
   set MINGW_PACKAGE_PREFIX=mingw-w64-x86_64
   set MINGW_PREFIX=c:\msys64\mingw64
   set MSYSTEM=MINGW64

   set PATH=%MINGW_PREFIX%\bin;c:\msys64\usr\bin;%PATH%

   rmdir /S /Q cpp\build
   mkdir cpp\build
   pushd cpp\build
   cmake -G "MSYS Makefiles" .. || exit /B
   make || exit /B
   popd

Debug builds
============

To build a Debug version of Arrow, you should have pre-installed a Debug
version of Boost. It's recommended to configure cmake build with the
following variables for Debug build:

* ``-DARROW_BOOST_USE_SHARED=OFF``: enables static linking with boost debug
  libs and simplifies run-time loading of 3rd parties
* ``-DBOOST_ROOT``: sets the root directory of boost libs. (Optional)
* ``-DBOOST_LIBRARYDIR``: sets the directory with boost lib files. (Optional)

The command line to build Arrow in Debug mode will look something like this:

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
====================================

Because Windows uses ``.lib`` files for both static and dynamic linking of
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
======================================

The Arrow headers on Windows static library builds (enabled by the CMake
option ``ARROW_BUILD_STATIC``) use the preprocessor macro ``ARROW_STATIC`` to
suppress dllimport/dllexport marking of symbols. Projects that statically link
against Arrow on Windows additionally need this definition. The Unix builds do
not use the macro.

Replicating Appveyor Builds
===========================

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
   @rem This is for static libraries needed for static_crt_build in appveyor
   .\b2 link=static -with-filesystem -with-regex -with-system install
   @rem this should put libraries and headers in c:\Boost

4. Activate anaconda/miniconda:

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
   SET ARROW_LLVM_VERSION=8.0.*
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
