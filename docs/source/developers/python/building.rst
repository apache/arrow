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

.. currentmodule:: pyarrow
.. highlight:: console
.. _build_pyarrow:

================
Building PyArrow
================


This page provides source build instructions for PyArrow for all platforms.

System Requirements
===================

.. tab-set::
   :sync-group: language

   .. tab-item:: Linux and macOS
      :sync: linux-macos

      On macOS, any modern XCode or Xcode Command Line Tools (``xcode-select --install``)
      are sufficient.

      On Linux, for this guide, we require a minimum of gcc or clang 9.
      You can check your version by running

      .. code-block::

         $ gcc --version

      If the system compiler is older than gcc 9, it can be set to a newer version
      using the ``$CC`` and ``$CXX`` environment variables:

      .. code-block::

         $ export CC=gcc-9
         $ export CXX=g++-9


   .. tab-item:: Windows
      :sync: wins

      Building on Windows requires one of the following compilers to be
      installed:

      - `Build Tools for Visual Studio 2022 <https://aka.ms/vs/17/release/vs_BuildTools.exe>`_ or
      - Visual Studio 2022

      During the setup of Build Tools, ensure at least one Windows SDK
      is selected.

Environment setup
=================

First, start from a fresh clone of Apache Arrow:

.. code-block::

   $ git clone https://github.com/apache/arrow.git

There are two supported ways to set up the build environment for PyArrow: using
**Conda** to manage the dependencies or using **pip** with manual dependency
management.

Both methods are shown bellow for Linux and macOS. For Windows, only the
Conda-based setup is currently documented, skipping some of the
Linux/macOS-only packages.

Note that in case you are not using conda on a Windows platform, Arrow C++
libraries need to be bundled with ``pyarrow``. For extra information see the
Windows tab under the :ref:`pyarrow_build_section` section.

.. tab-set::
   :sync-group: language

   .. tab-item:: Linux and macOS using conda
      :sync: linux-macos

      Pull in the test data and setup the environment variables:

      .. code-block::

         $ pushd arrow
         $ git submodule update --init
         $ export PARQUET_TEST_DATA="${PWD}/cpp/submodules/parquet-testing/data"
         $ export ARROW_TEST_DATA="${PWD}/testing/data"
         $ popd

      The `conda <https://conda.io/>`_ package manager allows installing build-time
      dependencies for Arrow C++ and PyArrow as pre-built binaries, which can make
      Arrow development easier and faster.

      Let's create a conda environment with all the C++ build and Python dependencies
      from conda-forge, targeting development for Python 3.13:

      .. code-block::

         $ conda create -y -n pyarrow-dev -c conda-forge \
               --file arrow/ci/conda_env_unix.txt \
               --file arrow/ci/conda_env_cpp.txt \
               --file arrow/ci/conda_env_python.txt \
               --file arrow/ci/conda_env_gandiva.txt \
               compilers \
               python=3.13 \
               pandas

      As of January 2019, the ``compilers`` package is needed on many Linux
      distributions to use packages from conda-forge.

      With this out of the way, you can now activate the conda environment

      .. code-block::

         $ conda activate pyarrow-dev


      We need to set some environment variables to let Arrow's build system know
      about our build toolchain:

      .. code-block::

         $ export ARROW_HOME=$CONDA_PREFIX


   .. tab-item:: Linux and macOS using pip

      .. warning::

         If you installed Python using the Anaconda distribution or `Miniconda
         <https://conda.io/miniconda.html>`_, you cannot currently use a
         pip-based virtual environment. Please follow the conda-based development
         instructions instead.

      Pull in the test data and setup the environment variables:

      .. code-block::

         $ pushd arrow
         $ git submodule update --init
         $ export PARQUET_TEST_DATA="${PWD}/cpp/submodules/parquet-testing/data"
         $ export ARROW_TEST_DATA="${PWD}/testing/data"
         $ popd

      **Using system and bundled dependencies**

      If not using conda, you must arrange for your system to provide the required
      build tools and dependencies.  Note that if some dependencies are absent,
      the Arrow C++ build chain may still be able to download and compile them
      on the fly, but this will take a longer time than with pre-installed binaries.

      On macOS, use Homebrew to install all dependencies required for
      building Arrow C++:

      .. code-block::

         $ brew update && brew bundle --file=arrow/cpp/Brewfile

      See :ref:`here <cpp-build-dependency-management>` for a list of dependencies you
      may need.

      On Debian/Ubuntu, you need the following minimal set of dependencies:

      .. code-block::

         $ sudo apt-get install build-essential ninja-build cmake python3-dev

      Now, let's create a Python virtual environment with all Python dependencies
      in the same folder as the repositories, and a target installation folder:

      .. code-block::

         $ python3 -m venv pyarrow-dev
         $ source ./pyarrow-dev/bin/activate
         $ pip install -r arrow/python/requirements-build.txt

         $ # This is the folder where we will install the Arrow libraries during
         $ # development
         $ mkdir dist

      If your CMake version is too old on Linux, you could get a newer one via
      ``pip install cmake``.

      We need to set some environment variables to let Arrow's build system know
      about our build toolchain:

      .. code-block::

         $ export ARROW_HOME=$(pwd)/dist
         $ export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH
         $ export CMAKE_PREFIX_PATH=$ARROW_HOME:$CMAKE_PREFIX_PATH

   .. tab-item:: Windows
      :sync: wins

      Let's create a conda environment with all the C++ build and Python dependencies
      from conda-forge, targeting development for Python 3.13:

      .. code-block::

         $ conda create -y -n pyarrow-dev -c conda-forge ^
               --file arrow\ci\conda_env_cpp.txt ^
               --file arrow\ci\conda_env_python.txt ^
               --file arrow\ci\conda_env_gandiva.txt ^
               python=3.13
         $ conda activate pyarrow-dev

      Now, we can build and install Arrow C++ libraries.

      We set the path of the installation directory of the Arrow C++
      libraries as ``ARROW_HOME``. When using a conda environment,
      Arrow C++ is installed in the environment directory, which path
      is saved in the `CONDA_PREFIX <https://docs.conda.io/projects/conda-build/en/latest/user-guide/environment-variables.html#environment-variables-that-affect-the-build-process>`_
      environment variable.

      .. code-block::

         $ set ARROW_HOME=%CONDA_PREFIX%\Library

Build
=====

First we need to configure, build and install the Arrow C++ libraries.
Then we can build PyArrow.

Build C++
---------

.. tab-set::
   :sync-group: language

   .. tab-item:: Linux and macOS
      :sync: linux-macos

      Now build the Arrow C++ libraries and install them into the directory we
      created above (stored in ``$ARROW_HOME``):

      .. code-block::

         $ cmake -S arrow/cpp -B arrow/cpp/build \
               -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
               --preset ninja-release-python
         $ cmake --build arrow/cpp/build --target install

      **About presets**

      ``ninja-release-python`` is not the only preset available - if you would like a
      build with more features like CUDA, Flight and Gandiva support you may opt for
      the ``ninja-release-python-maximal`` preset. If you wanted less features, (i.e.
      removing ORC and dataset support) you could opt for
      ``ninja-release-python-minimal``. Changing the word ``release`` to ``debug``
      with any of the aforementioned presets will generate a debug build of Arrow.

      **Individual components**

      The presets are provided as a convenience, but you may instead opt to
      specify the individual components:

      .. code-block::

         $ cmake -S arrow/cpp -B arrow/cpp/build \
               -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
               -DCMAKE_BUILD_TYPE=Debug \
               -DARROW_BUILD_TESTS=ON \
               -DARROW_COMPUTE=ON \
               -DARROW_CSV=ON \
               -DARROW_DATASET=ON \
               -DARROW_FILESYSTEM=ON \
               -DARROW_HDFS=ON \
               -DARROW_JSON=ON \
               -DARROW_PARQUET=ON \
               -DARROW_WITH_BROTLI=ON \
               -DARROW_WITH_BZ2=ON \
               -DARROW_WITH_LZ4=ON \
               -DARROW_WITH_SNAPPY=ON \
               -DARROW_WITH_ZLIB=ON \
               -DARROW_WITH_ZSTD=ON \
               -DPARQUET_REQUIRE_ENCRYPTION=ON
         $ cmake --build arrow/cpp/build --target install -j4

      If multiple versions of Python are installed in your environment, you may have
      to pass additional parameters to CMake so that it can find the right
      executable, headers and libraries.  For example, specifying
      ``-DPython3_EXECUTABLE=<path/to/bin/python>`` lets CMake choose the
      Python executable which you are using.

      .. note::

         On Linux systems with support for building on multiple architectures,
         ``make`` may install libraries in the ``lib64`` directory by default. For
         this reason we recommend passing ``-DCMAKE_INSTALL_LIBDIR=lib`` because the
         Python build scripts assume the library directory is ``lib``

      .. note::

         If you have conda installed but are not using it to manage dependencies,
         and you have trouble building the C++ library, you may need to set
         ``-DARROW_DEPENDENCY_SOURCE=AUTO`` or some other value (described
         :ref:`here <cpp-build-dependency-management>`)
         to explicitly tell CMake not to use conda.

   .. tab-item:: Windows
      :sync: wins

      There are presets provided as a convenience for building C++ (see Linux and macOS
      tab). Here we will instead specify the individual components:

      .. code-block::

         $ mkdir arrow\cpp\build
         $ pushd arrow\cpp\build
         $ cmake -G "Ninja" ^
               -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
               -DCMAKE_UNITY_BUILD=ON ^
               -DARROW_COMPUTE=ON ^
               -DARROW_CSV=ON ^
               -DARROW_CXXFLAGS="/WX /MP" ^
               -DARROW_DATASET=ON ^
               -DARROW_FILESYSTEM=ON ^
               -DARROW_HDFS=ON ^
               -DARROW_JSON=ON ^
               -DARROW_PARQUET=ON ^
               -DARROW_WITH_LZ4=ON ^
               -DARROW_WITH_SNAPPY=ON ^
               -DARROW_WITH_ZLIB=ON ^
               -DARROW_WITH_ZSTD=ON ^
               ..
         $ cmake --build . --target install --config Release
         $ popd

Optional build components
^^^^^^^^^^^^^^^^^^^^^^^^^

There are several optional components that can be enabled or disabled by setting
specific flags to ``ON`` or ``OFF``, respectively. See the list of
:ref:`python-dev-env-variables` below.

You may choose between different kinds of C++ build types:

* ``-DCMAKE_BUILD_TYPE=Release`` (the default) produces a build with optimizations
  enabled and debugging information disabled;
* ``-DCMAKE_BUILD_TYPE=Debug`` produces a build with optimizations
  disabled and debugging information enabled;
* ``-DCMAKE_BUILD_TYPE=RelWithDebInfo`` produces a build with both optimizations
  and debugging information enabled.

.. seealso::
   :ref:`Building Arrow C++ <cpp-building-building>`.

   For any other C++ build challenges, see :ref:`cpp-development`.

In case you may need to rebuild the C++ part due to errors in the process it is
advisable to delete the build folder, see :ref:`python-dev-env-variables`.
If the build has passed successfully and you need to rebuild due to latest pull
from git main, then this step is not needed.

.. _pyarrow_build_section:

Build PyArrow
-------------

If you did build one of the optional components in C++, the equivalent components
will be enabled by default for building pyarrow. This default can be overridden
by setting the corresponding ``PYARROW_WITH_$COMPONENT`` environment variable
to 0 or 1, see :ref:`python-dev-env-variables` below.

To build PyArrow run:

.. tab-set::
   :sync-group: language

   .. tab-item:: Linux and macOS
      :sync: linux-macos

      .. code-block::

         $ pushd arrow/python
         $ python setup.py build_ext --inplace
         $ popd

   .. tab-item:: Windows
      :sync: wins

      .. code-block::

         $ pushd arrow\python
         $ python setup.py build_ext --inplace
         $ popd

      .. note::

         If you are using Conda with Python 3.9 or earlier, you must
         set ``CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1``.

      .. note::

         With the above instructions the Arrow C++ libraries are not bundled with
         the Python extension. This is recommended for development as it allows the
         C++ libraries to be re-built separately.

         If you are using the conda package manager then conda will ensure the Arrow C++
         libraries are found. **In case you are NOT using conda** then you have to:

         * add the path of installed DLL libraries to ``PATH`` every time before
           importing ``pyarrow``, or
         * bundle the Arrow C++ libraries with ``pyarrow``.

         **Bundle Arrow C++ and PyArrow**

         If you want to bundle the Arrow C++ libraries with ``pyarrow``, set the
         ``PYARROW_BUNDLE_ARROW_CPP`` environment variable before building ``pyarrow``:

         .. code-block::

            $ set PYARROW_BUNDLE_ARROW_CPP=1
            $ python setup.py build_ext --inplace

         Note that bundled Arrow C++ libraries will not be automatically
         updated when rebuilding Arrow C++.

To set the number of threads used to compile PyArrow's C++/Cython components,
set the ``PYARROW_PARALLEL`` environment variable.

If you build PyArrow but then make changes to the Arrow C++ or PyArrow code,
you can end up with stale build artifacts. This can lead to
unexpected behavior or errors. To avoid this, you can clean the build
artifacts before rebuilding. See :ref:`python-dev-env-variables`.

By default, PyArrow will be built in release mode even if Arrow C++ has been
built in debug mode. To create a debug build of PyArrow, run
``export PYARROW_BUILD_TYPE=debug`` prior to running  ``python setup.py
build_ext --inplace`` above. A ``relwithdebinfo`` build can be created
similarly.

Self-Contained Wheel
^^^^^^^^^^^^^^^^^^^^

If you're preparing a PyArrow wheel for distribution (e.g., for PyPI), you’ll
need to build a self-contained wheel (including the Arrow and Parquet C++
libraries). This ensures that all necessary native libraries are bundled inside
the wheel, so users can install it without needing to have Arrow or Parquet
installed separately on their system.

To do this, pass the ``--bundle-arrow-cpp`` option to the build command:

.. code-block::

   $ pip install wheel  # if not installed
   $ python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
            --bundle-arrow-cpp bdist_wheel

This option is typically only needed for releases or distribution scenarios,
not for local development.

Editable install
^^^^^^^^^^^^^^^^

To install an editable PyArrow build, run the following command from the
``arrow/python`` directory:

.. code-block::

   pip install -e . --no-build-isolation``

This creates an *editable install*, meaning changes to the Python source code
will be reflected immediately without needing to reinstall the package.
The ``--no-build-isolation`` flag ensures that the build uses your current
environment's dependencies instead of creating an isolated one. This is
especially useful during development and debugging.

.. _stale_artifacts:

Deleting stale build artifacts
------------------------------

When there have been changes to the structure of the Arrow C++ library or PyArrow,
a thorough cleaning is recommended as a first attempt to fixing build errors.

.. note::

   It is not necessarily intuitive from the error itself that the problem is due to stale artifacts.
   Example of a build error from stale artifacts is
   ``Unknown CMake command "arrow_keep_backward_compatibility"``.

To delete stale Arrow C++ build artifacts:

.. code-block::

   $ rm -rf arrow/cpp/build

To delete stale PyArrow build artifacts:

.. code-block::

   $ git clean -Xfd python

If using a Conda environment, there are some build artifacts that get installed in
``$ARROW_HOME`` (aka ``$CONDA_PREFIX``). For example, ``$ARROW_HOME/lib/cmake/Arrow*``,
``$ARROW_HOME/include/arrow``, ``$ARROW_HOME/lib/libarrow*``, etc.

These files can be manually deleted. If unsure which files to erase, one approach
is to recreate the Conda environment.

Either delete the current one, and start fresh:

.. code-block::

   $ conda deactivate
   $ conda remove -n pyarrow-dev

Or, less destructively, create a different environment with a different name.

Docker examples
---------------

If you are having difficulty building the Python library from source, take a
look at the `python/examples/minimal_build <https://github.com/apache/arrow/tree/main/python/examples/minimal_build>`_
directory which illustrates a complete build and test from source both with
the conda- and pip-based build methods.

Test
====

Now you are ready to install test dependencies and run :ref:`python-unit-testing`, as
described in development section.

.. _python-dev-env-variables:

Relevant components and environment variables
=============================================

List of relevant environment variables that can be used to build
PyArrow are:

.. list-table::
   :widths: 20 20 20
   :header-rows: 1

   * - PyArrow environment variable
     - Description
     - Default value
   * - ``PYARROW_BUILD_TYPE``
     - Build type for PyArrow (release, debug or relwithdebinfo), sets ``CMAKE_BUILD_TYPE``
     - ``release``
   * - ``PYARROW_CMAKE_GENERATOR``
     - Example: ``'Visual Studio 17 2022 Win64'``
     - ``''``
   * - ``PYARROW_CMAKE_OPTIONS``
     - Extra CMake and Arrow options (ex. ``"-DARROW_SIMD_LEVEL=NONE -DCMAKE_OSX_ARCHITECTURES=x86_64;arm64"``)
     - ``''``
   * - ``PYARROW_CXXFLAGS``
     - Extra C++ compiler flags
     - ``''``
   * - ``PYARROW_GENERATE_COVERAGE``
     - Setting ``Xlinetrace`` flag to true for the Cython compiler
     - ``false``
   * - ``PYARROW_BUNDLE_ARROW_CPP``
     - Bundle the Arrow C++ libraries
     - ``0`` (``OFF``)
   * - ``PYARROW_BUNDLE_CYTHON_CPP``
     - Bundle the C++ files generated by Cython
     - ``0`` (``OFF``)
   * - ``PYARROW_BUILD_VERBOSE``
     - Enable verbose output from Makefile builds
     - ``0`` (``OFF``)
   * - ``PYARROW_PARALLEL``
     - Number of processes used to compile PyArrow’s C++/Cython components
     - ``''``

The components being disabled or enabled when building PyArrow is by default
based on how Arrow C++ is build (i.e. it follows the ``ARROW_$COMPONENT`` flags).
However, the ``PYARROW_WITH_$COMPONENT`` environment variables can still be used
to override this when building PyArrow (e.g. to disable components, or to enforce
certain components to be built):

.. list-table::
   :widths: 30 30
   :header-rows: 1

   * - Arrow flags/options
     - Corresponding environment variables for PyArrow
   * - ``ARROW_GCS``
     - ``PYARROW_WITH_GCS``
   * - ``ARROW_S3``
     - ``PYARROW_WITH_S3``
   * - ``ARROW_AZURE``
     - ``PYARROW_WITH_AZURE``
   * - ``ARROW_HDFS``
     - ``PYARROW_WITH_HDFS``
   * - ``ARROW_CUDA``
     - ``PYARROW_WITH_CUDA``
   * - ``ARROW_SUBSTRAIT``
     - ``PYARROW_WITH_SUBSTRAIT``
   * - ``ARROW_FLIGHT``
     - ``PYARROW_WITH_FLIGHT``
   * - ``ARROW_ACERO``
     - ``PYARROW_WITH_ACERO``
   * - ``ARROW_DATASET``
     - ``PYARROW_WITH_DATASET``
   * - ``ARROW_PARQUET``
     - ``PYARROW_WITH_PARQUET``
   * - ``PARQUET_REQUIRE_ENCRYPTION``
     - ``PYARROW_WITH_PARQUET_ENCRYPTION``
   * - ``ARROW_ORC``
     - ``PYARROW_WITH_ORC``
   * - ``ARROW_GANDIVA``
     - ``PYARROW_WITH_GANDIVA``

Installing Nightly Packages
===========================

.. warning::
    These packages are not official releases. Use them at your own risk.

PyArrow has nightly wheels for testing purposes hosted at
`scientific-python-nightly-wheels
<https://anaconda.org/scientific-python-nightly-wheels/pyarrow>`_.

These may be suitable for downstream libraries in their continuous integration
setup to maintain compatibility with the upcoming PyArrow features,
deprecations, and/or feature removals.

To install the most recent nightly version of PyArrow, run:

.. code-block:: bash

    pip install \
      -i https://pypi.anaconda.org/scientific-python-nightly-wheels/simple \
      pyarrow
