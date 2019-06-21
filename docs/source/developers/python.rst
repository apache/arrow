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
.. _python-development:

******************
Python Development
******************

This page provides general Python development guidelines and source build
instructions for all platforms.

Coding Style
============

We follow a similar PEP8-like coding style to the `pandas project
<https://github.com/pandas-dev/pandas>`_.

The code must pass ``flake8`` (available from pip or conda) or it will fail the
build. Check for style errors before submitting your pull request with:

.. code-block:: shell

   flake8 .
   flake8 --config=.flake8.cython .

The package ``autopep8`` (also available from pip or conda) can automatically
fix many of the errors reported by ``flake8``:

.. code-block:: shell

   autopep8 --in-place ../integration/integration_test.py
   autopep8 --in-place --global-config=.flake8.cython pyarrow/table.pxi

Unit Testing
============

We are using `pytest <https://docs.pytest.org/en/latest/>`_ to develop our unit
test suite. After building the project (see below) you can run its unit tests
like so:

.. code-block:: shell

   pytest pyarrow

Package requirements to run the unit tests are found in
``requirements-test.txt`` and can be installed if needed with ``pip install -r
requirements-test.txt``.

The project has a number of custom command line options for its test
suite. Some tests are disabled by default, for example. To see all the options,
run

.. code-block:: shell

   pytest pyarrow --help

and look for the "custom options" section.

Test Groups
-----------

We have many tests that are grouped together using pytest marks. Some of these
are disabled by default. To enable a test group, pass ``--$GROUP_NAME``,
e.g. ``--parquet``. To disable a test group, prepend ``disable``, so
``--disable-parquet`` for example. To run **only** the unit tests for a
particular group, prepend ``only-`` instead, for example ``--only-parquet``.

The test groups currently include:

* ``gandiva``: tests for Gandiva expression compiler (uses LLVM)
* ``hdfs``: tests that use libhdfs or libhdfs3 to access the Hadoop filesystem
* ``hypothesis``: tests that use the ``hypothesis`` module for generating
  random test cases. Note that ``--hypothesis`` doesn't work due to a quirk
  with pytest, so you have to pass ``--enable-hypothesis``
* ``large_memory``: Test requiring a large amount of system RAM
* ``orc``: Apache ORC tests
* ``parquet``: Apache Parquet tests
* ``plasma``: Plasma Object Store tests
* ``s3``: Tests for Amazon S3
* ``tensorflow``: Tests that involve TensorFlow

Benchmarking
------------

For running the benchmarks, see :ref:`python-benchmarks`.

Building on Linux and MacOS
=============================

System Requirements
-------------------

On macOS, any modern XCode (6.4 or higher; the current version is 10) is
sufficient.

On Linux, for this guide, we require a minimum of gcc 4.8, or clang 3.7 or
higher. You can check your version by running

.. code-block:: shell

   $ gcc --version

If the system compiler is older than gcc 4.8, it can be set to a newer version
using the ``$CC`` and ``$CXX`` environment variables:

.. code-block:: shell

   export CC=gcc-4.8
   export CXX=g++-4.8

Environment Setup and Build
---------------------------

First, let's clone the Arrow git repository:

.. code-block:: shell

   mkdir repos
   cd repos
   git clone https://github.com/apache/arrow.git

You should now see

.. code-block:: shell

   $ ls -l
   total 8
   drwxrwxr-x 12 wesm wesm 4096 Apr 15 19:19 arrow/

Using Conda
~~~~~~~~~~~

.. note::

   Using conda to build Arrow on macOS is complicated by the
   fact that the `conda-forge compilers require an older macOS SDK <https://stackoverflow.com/a/55798942>`_.
   Conda offers some `installation instructions <https://docs.conda.io/projects/conda-build/en/latest/resources/compiler-tools.html#macos-sdk>`_;
   the alternative would be to use :ref:`Homebrew <python-homebrew>` and
   ``pip`` instead.

Let's create a conda environment with all the C++ build and Python dependencies
from conda-forge, targeting development for Python 3.7:

On Linux and macOS:

.. code-block:: shell

    conda create -y -n pyarrow-dev -c conda-forge \
        --file arrow/ci/conda_env_unix.yml \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        compilers \
        python=3.7

As of January 2019, the ``compilers`` package is needed on many Linux
distributions to use packages from conda-forge.

With this out of the way, you can now activate the conda environment

.. code-block:: shell

   conda activate pyarrow-dev

For Windows, see the `Building on Windows`_ section below.

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_HOME=$CONDA_PREFIX

Using pip
~~~~~~~~~

.. warning::

   If you installed Python using the Anaconda distribution or `Miniconda
   <https://conda.io/miniconda.html>`_, you cannot currently use ``virtualenv``
   to manage your development. Please follow the conda-based development
   instructions instead.

.. _python-homebrew:

On macOS, use Homebrew to install all dependencies required for
building Arrow C++:

.. code-block:: shell

   brew update && brew bundle --file=arrow/cpp/Brewfile

See :ref:`here <cpp-build-dependency-management>` for a list of dependencies you
may need.

On Debian/Ubuntu, you need the following minimal set of dependencies. All other
dependencies will be automatically built by Arrow's third-party toolchain.

.. code-block:: shell

   $ sudo apt-get install libjemalloc-dev libboost-dev \
                          libboost-filesystem-dev \
                          libboost-system-dev \
                          libboost-regex-dev \
                          python-dev \
                          autoconf \
                          flex \
                          bison

If you are building Arrow for Python 3, install ``python3-dev`` instead of ``python-dev``.

On Arch Linux, you can get these dependencies via pacman.

.. code-block:: shell

   $ sudo pacman -S jemalloc boost

Now, let's create a Python virtualenv with all Python dependencies in the same
folder as the repositories and a target installation folder:

.. code-block:: shell

   virtualenv pyarrow
   source ./pyarrow/bin/activate
   pip install six numpy pandas cython pytest hypothesis

   # This is the folder where we will install the Arrow libraries during
   # development
   mkdir dist

If your cmake version is too old on Linux, you could get a newer one via
``pip install cmake``.

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_HOME=$(pwd)/dist
   export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH

Build and test
--------------

Now build and install the Arrow C++ libraries:

.. code-block:: shell

   mkdir arrow/cpp/build
   pushd arrow/cpp/build

   cmake -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
         -DCMAKE_INSTALL_LIBDIR=lib \
         -DARROW_FLIGHT=ON \
         -DARROW_GANDIVA=ON \
         -DARROW_ORC=ON \
         -DARROW_PARQUET=ON \
         -DARROW_PYTHON=ON \
         -DARROW_PLASMA=ON \
         -DARROW_BUILD_TESTS=ON \
         ..
   make -j4
   make install
   popd

Many of these components are optional, and can be switched off by setting them
to ``OFF``:

* ``ARROW_FLIGHT``: RPC framework
* ``ARROW_GANDIVA``: LLVM-based expression compiler
* ``ARROW_ORC``: Support for Apache ORC file format
* ``ARROW_PARQUET``: Support for Apache Parquet file format
* ``ARROW_PLASMA``: Shared memory object store

If multiple versions of Python are installed in your environment, you may have
to pass additional parameters to cmake so that it can find the right
executable, headers and libraries.  For example, specifying
``-DPYTHON_EXECUTABLE=$VIRTUAL_ENV/bin/python`` (assuming that you're in
virtualenv) enables cmake to choose the python executable which you are using.

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

Now, build pyarrow:

.. code-block:: shell

   pushd arrow/python
   export PYARROW_WITH_FLIGHT=1
   export PYARROW_WITH_GANDIVA=1
   export PYARROW_WITH_ORC=1
   export PYARROW_WITH_PARQUET=1
   python setup.py build_ext --inplace
   popd

If you did not build one of the optional components, set the corresponding
``PYARROW_WITH_$COMPONENT`` environment variable to 0.

Now you are ready to install test dependencies and run `Unit Testing`_, as
described above.

To build a self-contained wheel (including the Arrow and Parquet C++
libraries), one can set ``--bundle-arrow-cpp``:

.. code-block:: shell

   pip install wheel  # if not installed
   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --bundle-arrow-cpp bdist_wheel

Building with CUDA support
~~~~~~~~~~~~~~~~~~~~~~~~~~

The :mod:`pyarrow.cuda` module offers support for using Arrow platform
components with Nvidia's CUDA-enabled GPU devices. To build with this support,
pass ``-DARROW_CUDA=ON`` when building the C++ libraries, and set the following
environment variable when building pyarrow:

.. code-block:: shell

   export PYARROW_WITH_CUDA=1

Debugging
---------

Since pyarrow depends on the Arrow C++ libraries, debugging can
frequently involve crossing between Python and C++ shared libraries.

Using gdb on Linux
~~~~~~~~~~~~~~~~~~

To debug the C++ libraries with gdb while running the Python unit
   test, first start pytest with gdb:

.. code-block:: shell

   gdb --args python -m pytest pyarrow/tests/test_to_run.py -k $TEST_TO_MATCH

To set a breakpoint, use the same gdb syntax that you would when
debugging a C++ unitttest, for example:

.. code-block:: shell

   (gdb) b src/arrow/python/arrow_to_pandas.cc:1874
   No source file named src/arrow/python/arrow_to_pandas.cc.
   Make breakpoint pending on future shared library load? (y or [n]) y
   Breakpoint 1 (src/arrow/python/arrow_to_pandas.cc:1874) pending.

Building on Windows
===================

First, we bootstrap a conda environment similar to above, but skipping some of
the Linux/macOS-only packages:

First, starting from fresh clones of Apache Arrow:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git

.. code-block:: shell

    conda create -y -n pyarrow-dev -c conda-forge ^
        --file arrow\ci\conda_env_cpp.yml ^
        --file arrow\ci\conda_env_python.yml ^
        python=3.7
   conda activate pyarrow-dev

Now, we build and install Arrow C++ libraries

.. code-block:: shell

   mkdir cpp\build
   cd cpp\build
   set ARROW_HOME=C:\thirdparty
   cmake -G "Visual Studio 14 2015 Win64" ^
         -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
         -DARROW_CXXFLAGS="/WX /MP" ^
         -DARROW_GANDIVA=on ^
         -DARROW_PARQUET=on ^
         -DARROW_PYTHON=on ..
   cmake --build . --target INSTALL --config Release
   cd ..\..

After that, we must put the install directory's bin path in our ``%PATH%``:

.. code-block:: shell

   set PATH=%ARROW_HOME%\bin;%PATH%

Now, we can build pyarrow:

.. code-block:: shell

   cd python
   set PYARROW_WITH_GANDIVA=1
   set PYARROW_WITH_PARQUET=1
   python setup.py build_ext --inplace

Then run the unit tests with:

.. code-block:: shell

   py.test pyarrow -v

Running C++ unit tests for Python integration
---------------------------------------------

Getting ``python-test.exe`` to run is a bit tricky because your
``%PYTHONHOME%`` must be configured to point to the active conda environment:

.. code-block:: shell

   set PYTHONHOME=%CONDA_PREFIX%

Now ``python-test.exe`` or simply ``ctest`` (to run all tests) should work.

Windows Caveats
---------------

Some components are not supported yet on Windows:

* Flight RPC
* Plasma
