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
.. _python-development:

==================
Python Development
==================

This page provides general Python development guidelines and source build
instructions for all platforms.

.. _python-coding-style:

Coding Style
============

We follow a similar PEP8-like coding style to the `pandas project
<https://github.com/pandas-dev/pandas>`_.  To fix style issues, use the
``pre-commit`` command:

.. code-block::

   $ pre-commit run --show-diff-on-failure --color=always --all-files python

.. _python-unit-testing:

Unit Testing
============

We are using `pytest <https://docs.pytest.org/en/latest/>`_ to develop our unit
test suite. After building the project (see below) you can run its unit tests
like so:

.. code-block::

   $ pushd arrow/python
   $ python -m pytest pyarrow
   $ popd

Package requirements to run the unit tests are found in
``requirements-test.txt`` and can be installed if needed with ``pip install -r
requirements-test.txt``.

If you get import errors for ``pyarrow._lib`` or another PyArrow module when
trying to run the tests, run ``python -m pytest arrow/python/pyarrow`` and check
if the editable version of pyarrow was installed correctly.

The project has a number of custom command line options for its test
suite. Some tests are disabled by default, for example. To see all the options,
run

.. code-block::

   $ python -m pytest pyarrow --help

and look for the "custom options" section.

.. note::

   There are a few low-level tests written directly in C++. These tests are
   implemented in `pyarrow/src/arrow/python/python_test.cc <https://github.com/apache/arrow/blob/main/python/pyarrow/src/arrow/python/python_test.cc>`_,
   but they are also wrapped in a ``pytest``-based
   `test module <https://github.com/apache/arrow/blob/main/python/pyarrow/tests/test_cpp_internals.py>`_
   run automatically as part of the PyArrow test suite.

Test Groups
-----------

We have many tests that are grouped together using pytest marks. Some of these
are disabled by default. To enable a test group, pass ``--$GROUP_NAME``,
e.g. ``--parquet``. To disable a test group, prepend ``disable``, so
``--disable-parquet`` for example. To run **only** the unit tests for a
particular group, prepend ``only-`` instead, for example ``--only-parquet``.

The test groups currently include:

* ``dataset``: Apache Arrow Dataset tests
* ``flight``: Flight RPC tests
* ``gandiva``: tests for Gandiva expression compiler (uses LLVM)
* ``hdfs``: tests that use libhdfs to access the Hadoop filesystem
* ``hypothesis``: tests that use the ``hypothesis`` module for generating
  random test cases. Note that ``--hypothesis`` doesn't work due to a quirk
  with pytest, so you have to pass ``--enable-hypothesis``
* ``large_memory``: Test requiring a large amount of system RAM
* ``orc``: Apache ORC tests
* ``parquet``: Apache Parquet tests
* ``s3``: Tests for Amazon S3
* ``tensorflow``: Tests that involve TensorFlow

Doctest
-------

We are using `doctest <https://docs.python.org/3/library/doctest.html>`_
to check that docstring examples are up-to-date and correct. You can
also do that locally by running:

.. code-block::

   $ pushd arrow/python
   $ python -m pytest --doctest-modules
   $ python -m pytest --doctest-modules path/to/module.py # checking single file
   $ popd

for ``.py`` files or

.. code-block::

   $ pushd arrow/python
   $ python -m pytest --doctest-cython
   $ python -m pytest --doctest-cython path/to/module.pyx # checking single file
   $ popd

for ``.pyx`` and ``.pxi`` files. In this case you will also need to
install the `pytest-cython <https://github.com/lgpage/pytest-cython>`_ plugin.

Benchmarking
------------

For running the benchmarks, see :ref:`python-benchmarks`.

.. _build_pyarrow:

Building on Linux and macOS
===========================

System Requirements
-------------------

On macOS, any modern XCode (6.4 or higher; the current version is 13) or
Xcode Command Line Tools (``xcode-select --install``) are sufficient.

On Linux, for this guide, we require a minimum of gcc 4.8 or clang 3.7.
You can check your version by running

.. code-block::

   $ gcc --version

If the system compiler is older than gcc 4.8, it can be set to a newer version
using the ``$CC`` and ``$CXX`` environment variables:

.. code-block::

   $ export CC=gcc-4.8
   $ export CXX=g++-4.8

Environment Setup and Build
---------------------------

First, let's clone the Arrow git repository:

.. code-block::

   $ git clone https://github.com/apache/arrow.git

Pull in the test data and setup the environment variables:

.. code-block::

   $ pushd arrow
   $ git submodule update --init
   $ export PARQUET_TEST_DATA="${PWD}/cpp/submodules/parquet-testing/data"
   $ export ARROW_TEST_DATA="${PWD}/testing/data"
   $ popd

Using Conda
~~~~~~~~~~~

The `conda <https://conda.io/>`_ package manager allows installing build-time
dependencies for Arrow C++ and PyArrow as pre-built binaries, which can make
Arrow development easier and faster.

Let's create a conda environment with all the C++ build and Python dependencies
from conda-forge, targeting development for Python 3.10:

On Linux and macOS:

.. code-block::

   $ conda create -y -n pyarrow-dev -c conda-forge \
          --file arrow/ci/conda_env_unix.txt \
          --file arrow/ci/conda_env_cpp.txt \
          --file arrow/ci/conda_env_python.txt \
          --file arrow/ci/conda_env_gandiva.txt \
          compilers \
          python=3.10 \
          pandas

As of January 2019, the ``compilers`` package is needed on many Linux
distributions to use packages from conda-forge.

With this out of the way, you can now activate the conda environment

.. code-block::

   $ conda activate pyarrow-dev

For Windows, see the `Building on Windows`_ section below.

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block::

   $ export ARROW_HOME=$CONDA_PREFIX

Using system and bundled dependencies
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

   If you installed Python using the Anaconda distribution or `Miniconda
   <https://conda.io/miniconda.html>`_, you cannot currently use a
   pip-based virtual environment. Please follow the conda-based development
   instructions instead.

If not using conda, you must arrange for your system to provide the required
build tools and dependencies.  Note that if some dependencies are absent,
the Arrow C++ build chain may still be able to download and compile them
on the fly, but this will take a longer time than with pre-installed binaries.

.. _python-homebrew:

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

Build and test
--------------

Now build the Arrow C++ libraries and install them into the directory we
created above (stored in ``$ARROW_HOME``):

.. code-block::

   $ cmake -S arrow/cpp -B arrow/cpp/build \
           -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
           --preset ninja-release-python
   $ cmake --build arrow/cpp/build --target install

``ninja-release-python`` is not the only preset available - if you would like a
build with more features like CUDA, Flight and Gandiva support you may opt for
the ``ninja-release-python-maximal`` preset. If you wanted less features, (i.e.
removing ORC and dataset support) you could opt for
``ninja-release-python-minimal``. Changing the word ``release`` to ``debug``
with any of the aforementioned presets will generate a debug build of Arrow.

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

There are a number of optional components that can be switched ON by
adding flags with ``ON``:

* ``ARROW_CUDA``: Support for CUDA-enabled GPUs
* ``ARROW_DATASET``: Support for Apache Arrow Dataset
* ``ARROW_FLIGHT``: Flight RPC framework
* ``ARROW_GANDIVA``: LLVM-based expression compiler
* ``ARROW_ORC``: Support for Apache ORC file format
* ``ARROW_PARQUET``: Support for Apache Parquet file format
* ``PARQUET_REQUIRE_ENCRYPTION``: Support for Parquet Modular Encryption

Anything set to ``ON`` above can also be turned off. Note that some compression
libraries are recommended for full Parquet support.

You may choose between different kinds of C++ build types:

* ``-DCMAKE_BUILD_TYPE=Release`` (the default) produces a build with optimizations
  enabled and debugging information disabled;
* ``-DCMAKE_BUILD_TYPE=Debug`` produces a build with optimizations
  disabled and debugging information enabled;
* ``-DCMAKE_BUILD_TYPE=RelWithDebInfo`` produces a build with both optimizations
  and debugging information enabled.

.. seealso::
   :ref:`Building Arrow C++ <cpp-building-building>`.

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

For any other C++ build challenges, see :ref:`cpp-development`.

In case you may need to rebuild the C++ part due to errors in the process it is
advisable to delete the build folder with command ``rm -rf arrow/cpp/build``.
If the build has passed successfully and you need to rebuild due to latest pull
from git main, then this step is not needed.

Now, build pyarrow:

.. code-block::

   $ pushd arrow/python
   $ export PYARROW_PARALLEL=4
   $ python setup.py build_ext --inplace
   $ popd

If you did build one of the optional components in C++, the equivalent components
will be enabled by default for building pyarrow. This default can be overridden
by setting the corresponding ``PYARROW_WITH_$COMPONENT`` environment variable
to 0 or 1, see :ref:`python-dev-env-variables` below.

To set the number of threads used to compile PyArrow's C++/Cython components,
set the ``PYARROW_PARALLEL`` environment variable.

If you build PyArrow but then make changes to the Arrow C++ or PyArrow code,
you can end up with stale build artifacts. This can lead to
unexpected behavior or errors. To avoid this, you can clean the build
artifacts before rebuilding. You can do this by running:

.. code-block::

   $ pushd arrow/python
   $ git clean -Xfd .

By default, PyArrow will be built in release mode even if Arrow C++ has been
built in debug mode. To create a debug build of PyArrow, run
``export PYARROW_BUILD_TYPE=debug`` prior to running  ``python setup.py
build_ext --inplace`` above. A ``relwithdebinfo`` build can be created
similarly.

Now you are ready to install test dependencies and run `Unit Testing`_, as
described above.

If you need to build a self-contained wheel (including the Arrow and Parquet C++
libraries), you can set ``--bundle-arrow-cpp``:

.. code-block::

   $ pip install wheel  # if not installed
   $ python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
            --bundle-arrow-cpp bdist_wheel

.. note::
   To install an editable PyArrow build run ``pip install -e . --no-build-isolation``
   in the ``arrow/python`` directory.

Docker examples
~~~~~~~~~~~~~~~

If you are having difficulty building the Python library from source, take a
look at the `python/examples/minimal_build <https://github.com/apache/arrow/tree/main/python/examples/minimal_build>`_
directory which illustrates a complete build and test from source both with
the conda- and pip-based build methods.

Debugging
---------

Since pyarrow depends on the Arrow C++ libraries, debugging can
frequently involve crossing between Python and C++ shared libraries.
For the best experience, make sure you've built both Arrow C++
(``-DCMAKE_BUILD_TYPE=Debug``) and PyArrow (``export PYARROW_BUILD_TYPE=debug``)
in debug mode.

Using gdb on Linux
~~~~~~~~~~~~~~~~~~

To debug the C++ libraries with gdb while running the Python unit
tests, first start pytest with gdb:

.. code-block::

   $ gdb --args python -m pytest pyarrow/tests/test_to_run.py -k $TEST_TO_MATCH

To set a breakpoint, use the same gdb syntax that you would when
debugging a C++ program, for example:

.. code-block::

   (gdb) b src/arrow/python/arrow_to_pandas.cc:1874
   No source file named src/arrow/python/arrow_to_pandas.cc.
   Make breakpoint pending on future shared library load? (y or [n]) y
   Breakpoint 1 (src/arrow/python/arrow_to_pandas.cc:1874) pending.

.. seealso::

   The :ref:`GDB extension for Arrow C++ <cpp_gdb_extension>`.

.. _build_pyarrow_win:

Building on Windows
===================

Building on Windows requires one of the following compilers to be installed:

- `Build Tools for Visual Studio 2017 <https://download.visualstudio.microsoft.com/download/pr/3e542575-929e-4297-b6c6-bef34d0ee648/639c868e1219c651793aff537a1d3b77/vs_buildtools.exe>`_
- Visual Studio 2017

During the setup of Build Tools, ensure at least one Windows SDK is selected.

We bootstrap a conda environment similar to above, but skipping some of the
Linux/macOS-only packages:

First, starting from a fresh clone of Apache Arrow:

.. code-block::

   $ git clone https://github.com/apache/arrow.git

.. code-block::

   $ conda create -y -n pyarrow-dev -c conda-forge ^
         --file arrow\ci\conda_env_cpp.txt ^
         --file arrow\ci\conda_env_python.txt ^
         --file arrow\ci\conda_env_gandiva.txt ^
         python=3.10
   $ conda activate pyarrow-dev

Now, we build and install Arrow C++ libraries.

We set the path of the installation directory of the Arrow C++ libraries as
``ARROW_HOME``. When using a conda environment, Arrow C++ is installed
in the environment directory, which path is saved in the
`CONDA_PREFIX <https://docs.conda.io/projects/conda-build/en/latest/user-guide/environment-variables.html#environment-variables-that-affect-the-build-process>`_
environment variable.

.. code-block::

   $ set ARROW_HOME=%CONDA_PREFIX%\Library

Let's configure, build and install the Arrow C++ libraries:

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

Now, we can build pyarrow:

.. code-block::

   $ pushd arrow\python
   $ set CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1
   $ python setup.py build_ext --inplace
   $ popd

.. note::

   For building pyarrow, the above defined environment variables need to also
   be set. Remember this if to want to re-build ``pyarrow`` after your initial build.

.. note::

   If you are using Conda with Python 3.9 or earlier, you must
   set ``CONDA_DLL_SEARCH_MODIFICATION_ENABLE=1``.

Then run the unit tests with:

.. code-block::

   $ pushd arrow\python
   $ python -m pytest pyarrow
   $ popd

.. note::

   With the above instructions the Arrow C++ libraries are not bundled with
   the Python extension. This is recommended for development as it allows the
   C++ libraries to be re-built separately.

   If you are using the conda package manager then conda will ensure the Arrow C++
   libraries are found. In case you are *not* using conda then you have to:

   * add the path of installed DLL libraries to ``PATH`` every time before
     importing ``pyarrow``, or
   * bundle the Arrow C++ libraries with ``pyarrow``.

   If you want to bundle the Arrow C++ libraries with ``pyarrow``, set the
   ``PYARROW_BUNDLE_ARROW_CPP`` environment variable before building ``pyarrow``:

   .. code-block::

      $ set PYARROW_BUNDLE_ARROW_CPP=1
      $ python setup.py build_ext --inplace

   Note that bundled Arrow C++ libraries will not be automatically
   updated when rebuilding Arrow C++.

Caveats
-------

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
     - Example: ``'Visual Studio 15 2017 Win64'``
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

Deleting stale build artifacts
==============================

When there have been changes to the structure of the Arrow C++ library or PyArrow,
a thorough cleaning is recommended as a first attempt to fixing build errors.

.. note::

   It is not necessarily intuitive from the error itself that the problem is due to stale artifacts.
   Example of a build error from stale artifacts is "Unknown CMake command "arrow_keep_backward_compatibility"".

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
