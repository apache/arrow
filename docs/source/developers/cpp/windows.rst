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

.. _windows-system-setup:

System Setup
============

Microsoft provides the free Visual Studio Community edition. When doing
development in the shell, you must initialize the development environment
each time you open the shell.

For Visual Studio 2017, execute the following batch script:

.. code-block:: shell

   "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64

For Visual Studio 2019, the script is:

.. code-block:: shell

  "C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\Common7\Tools\VsDevCmd.bat" -arch=amd64

One can configure a console emulator like `cmder <https://cmder.net/>`_ to
automatically launch this when starting a new development console.

Using conda-forge for build dependencies
========================================

`Miniconda <https://conda.io/miniconda.html>`_ is a minimal Python distribution
including the `conda <https://conda.io>`_ package manager. Some members of the
Apache Arrow community participate in the maintenance of `conda-forge
<https://conda-forge.org/>`_, a community-maintained cross-platform package
repository for conda.

To use ``conda-forge`` for your C++ build dependencies on Windows, first
download and install a 64-bit distribution from the `Miniconda homepage
<https://conda.io/miniconda.html>`_

To configure ``conda`` to use the ``conda-forge`` channel by default, launch a
command prompt (``cmd.exe``), run the initialization command shown
:ref:`above<windows-system-setup>` (``vcvarsall.bat`` or ``VsDevCmd.bat``), then
run the command:

.. code-block:: shell

   conda config --add channels conda-forge

Now, you can bootstrap a build environment (call from the root directory of the
Arrow codebase):

.. code-block:: shell

   conda create -y -n arrow-dev --file=ci\conda_env_cpp.txt

Then "activate" this conda environment with:

.. code-block:: shell

   activate arrow-dev

If the environment has been activated, the Arrow build system will
automatically see the ``%CONDA_PREFIX%`` environment variable and use that for
resolving the build dependencies. This is equivalent to setting

.. code-block:: shell

   -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
   -DARROW_PACKAGE_PREFIX=%CONDA_PREFIX%\Library

To use the Visual Studio IDE with this conda environment activated, launch it by
running the command ``devenv`` from the same command prompt.

Note that dependencies installed as conda packages are built in release mode and
cannot link with debug builds. If you intend to use ``-DCMAKE_BUILD_TYPE=debug``
then you must build the packages from source.
``-DCMAKE_BUILD_TYPE=relwithdebinfo`` is also available, which produces a build
that can both be linked with release libraries and be debugged.

.. note::

   If you run into any problems using conda packages for dependencies, a very
   common problem is mixing packages from the ``defaults`` channel with those
   from ``conda-forge``. You can examine the installed packages in your
   environment (and their origin) with ``conda list``

Using vcpkg for build dependencies
========================================

`vcpkg <https://github.com/microsoft/vcpkg>`_ is an open source package manager
from Microsoft. It hosts community-contributed ports of C and C++ packages and
their dependencies. Arrow includes a manifest file `cpp/vcpkg.json
<https://github.com/apache/arrow/blob/main/cpp/vcpkg.json>`_ that specifies
which vcpkg packages are required to build the C++ library.

To use vcpkg for C++ build dependencies on Windows, first
`install <https://docs.microsoft.com/en-us/cpp/build/install-vcpkg>`_ and
`integrate <https://docs.microsoft.com/en-us/cpp/build/integrate-vcpkg>`_
vcpkg. Then change working directory in ``cmd.exe`` to the root directory
of Arrow and run the command:

.. code-block:: shell

   vcpkg install ^
     --triplet x64-windows ^
     --x-manifest-root cpp  ^
     --feature-flags=versions ^
     --clean-after-build

On Windows, vcpkg builds dynamic link libraries by default. Use the triplet
``x64-windows-static`` to build static libraries. vcpkg downloads source
packages and compiles them locally, so installing dependencies with vcpkg is
more time-consuming than with conda.

Then in your ``cmake`` command, to use dependencies installed by vcpkg, set:

.. code-block:: shell

   -DARROW_DEPENDENCY_SOURCE=VCPKG

You can optionally set other variables to override the default CMake
configurations for vcpkg, including:

* ``-DCMAKE_TOOLCHAIN_FILE``: by default, the CMake scripts automatically find
  the location of the vcpkg CMake toolchain file ``vcpkg.cmake``; use this to
  instead specify its location
* ``-DVCPKG_TARGET_TRIPLET``: by default, the CMake scripts attempt to infer the
  vcpkg
  `triplet <https://github.com/microsoft/vcpkg/blob/master/docs/users/triplets.md>`_;
  use this to instead specify the triplet
* ``-DARROW_DEPENDENCY_USE_SHARED``: default is ``ON``; set to ``OFF`` for
  static libraries
* ``-DVCPKG_MANIFEST_MODE``: default is ``ON``; set to ``OFF`` to ignore the
  ``vcpkg.json`` manifest file and only look for vcpkg packages that are
  already installed under the directory where vcpkg is installed


Building using Visual Studio (MSVC) Solution Files
==================================================

Change working directory in ``cmd.exe`` to the root directory of Arrow and do
an out of source build by generating a MSVC solution:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake .. -G "Visual Studio 15 2017" -A x64 ^
         -DARROW_BUILD_TESTS=ON
   cmake --build . --config Release

For newer versions of Visual Studio, specify the generator
``Visual Studio 16 2019`` or see ``cmake --help`` for available
generators.

Building with Ninja and sccache
===============================

The `Ninja <https://ninja-build.org/>`_ build system offers better build
parallelization, and the optional `sccache
<https://github.com/mozilla/sccache#local>`_ compiler cache keeps track of
past compilations to avoid running them over and over again (in a way similar
to the Unix-specific ``ccache``).

Newer versions of Visual Studio include Ninja. To see if your Visual Studio
includes Ninja, run the initialization command shown
:ref:`above<windows-system-setup>` (``vcvarsall.bat`` or ``VsDevCmd.bat``), then
run ``ninja --version``.

If Ninja is not included in your version of Visual Studio, and you are using
conda, activate your conda environment and install Ninja:

.. code-block:: shell

   activate arrow-dev
   conda install -c conda-forge ninja

If you are not using conda,
`install Ninja from another source <https://github.com/ninja-build/ninja/wiki/Pre-built-Ninja-packages>`_
.

After installation is complete, change working directory in ``cmd.exe`` to the root directory of Arrow and
do an out of source build by generating Ninja files:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake -G "Ninja" ^
         -DARROW_BUILD_TESTS=ON ^
         -DGTest_SOURCE=BUNDLED ..
   cmake --build . --config Release

To use ``sccache`` in local storage mode you need to set ``SCCACHE_DIR``
environment variable before calling ``cmake``:

.. code-block:: shell

   ...
   set SCCACHE_DIR=%LOCALAPPDATA%\Mozilla\sccache
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

Building on Windows/ARM64 using Ninja and Clang
===============================================

Ninja and clang can be used for building library on windows/arm64 platform.

.. code-block:: batch

   cd cpp
   mkdir build
   cd build

   set CC=clang-cl
   set CXX=clang-cl

   cmake -G "Ninja" ..

   cmake --build . --config Release

LLVM toolchain for Windows on ARM64 can be downloaded from LLVM release page `LLVM release page <https://releases.llvm.org>`_

Visual Studio (MSVC) cannot be yet used for compiling win/arm64 build due to compatibility issues for dependencies like xsimd and boost library.

Note: This is only an experimental build for WoA64 as all features are not extensively tested through CI due to lack of infrastructure.

Debug builds
============

To build a Debug version of Arrow, you should have pre-installed a Debug
version of Boost. It's recommended to configure ``cmake`` with the following
variables for Debug build:

* ``-DARROW_BOOST_USE_SHARED=OFF``: enables static linking with boost debug
  libs and simplifies run-time loading of 3rd parties
* ``-DBOOST_ROOT``: sets the root directory of boost libs. (Optional)
* ``-DBOOST_LIBRARYDIR``: sets the directory with boost lib files. (Optional)

The command line to build Arrow in Debug mode will look something like this:

.. code-block:: shell

   cd cpp
   mkdir build
   cd build
   cmake .. -G "Visual Studio 15 2017" -A x64 ^
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
<https://github.com/apache/arrow/blob/main/ci/appveyor-cpp-build.bat>`_,
which is used by automated Appveyor builds.

Statically linking to Arrow on Windows
======================================

The Arrow headers on Windows static library builds (enabled by the CMake
option ``ARROW_BUILD_STATIC``) use the preprocessor macro ``ARROW_STATIC`` to
suppress dllimport/dllexport marking of symbols. Projects that statically link
against Arrow on Windows additionally need this definition. The Unix builds do
not use the macro.

In addition if using ``-DARROW_FLIGHT=ON``, ``ARROW_FLIGHT_STATIC`` needs to
be defined, and similarly for ``-DARROW_FLIGHT_SQL=ON``.

.. code-block:: cmake

   project(MyExample)

   find_package(Arrow REQUIRED)

   add_executable(my_example my_example.cc)
   target_link_libraries(my_example
                         PRIVATE
                         arrow_static
                         arrow_flight_static
                         arrow_flight_sql_static)

   target_compile_definitions(my_example
                              PUBLIC
                              ARROW_STATIC
                              ARROW_FLIGHT_STATIC
                              ARROW_FLIGHT_SQL_STATIC)

Downloading the Timezone Database
=================================

To run some of the compute unit tests on Windows, the IANA timezone database
and the Windows timezone mapping need to be downloaded first. See 
:ref:`download-timezone-database` for download instructions. To set a non-default
path for the timezone database while running the unit tests, set the 
``ARROW_TIMEZONE_DATABASE`` environment variable.

Replicating Appveyor Builds
===========================

For people more familiar with linux development but need to replicate a failing
appveyor build, here are some rough notes from replicating the
``Static_Crt_Build`` (make unittest will probably still fail but many unit
tests can be made with there individual make targets).

1. Microsoft offers trial VMs for `Windows with Microsoft Visual Studio
   <https://developer.microsoft.com/en-us/windows/downloads/virtual-machines>`_.
   Download and install a version.
2. Run the VM and install `Git <https://git-scm.com/>`_, `CMake
   <https://cmake.org/>`_, and Miniconda or Anaconda (these instructions assume
   Anaconda). Also install the `"Build Tools for Visual Studio"
   <https://visualstudio.microsoft.com/downloads/#build-tools-for-visual-studio-2019>`_.
   Make sure to select the C++ toolchain in the installer wizard, and reboot
   after installation.
3. Download `pre-built Boost debug binaries
   <https://sourceforge.net/projects/boost/files/boost-binaries/>`_ and install
   it.

   Run this from an Anaconda/Miniconda command prompt (*not* PowerShell prompt),
   and make sure to run "vcvarsall.bat x64" first. The location of vcvarsall.bat
   will depend, it may be under a different path than commonly indicated,
   e.g. "``C:\Program Files (x86)\Microsoft Visual Studio\2019\BuildTools\VC\Auxiliary\Build\vcvarsall.bat``"
   with the 2019 build tools.

.. code-block:: shell

   cd $EXTRACT_BOOST_DIRECTORY
   .\bootstrap.bat
   @rem This is for static libraries needed for static_crt_build in appveyor
   .\b2 link=static --with-filesystem --with-regex --with-system install
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
   SET PYTHON=3.9
   SET ARCH=64
   SET PATH=C:\Users\User\Anaconda3;C:\Users\User\Anaconda3\Scripts;C:\Users\User\Anaconda3\Library\bin;%PATH%
   SET BOOST_LIBRARYDIR=C:\Boost\lib
   SET BOOST_ROOT=C:\Boost

7. Run appveyor scripts:

.. code-block:: shell

   conda install -c conda-forge --file .\ci\conda_env_cpp.txt
   .\ci\appveyor-cpp-setup.bat
   @rem this might fail but at this point most unit tests should be buildable by there individual targets
   @rem see next line for example.
   .\ci\appveyor-cpp-build.bat
   @rem you can also just invoke cmake directly with the desired options
   cmake --build . --config Release --target arrow-compute-hash-test
