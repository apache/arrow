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
.. _development:

***********
Development
***********

Developing on Linux and MacOS
=============================

System Requirements
-------------------

On macOS, any modern XCode (6.4 or higher; the current version is 8.3.1) is
sufficient.

On Linux, for this guide, we recommend using gcc 4.8 or 4.9, or clang 3.7 or
higher. You can check your version by running

.. code-block:: shell

   $ gcc --version

On Ubuntu 16.04 and higher, you can obtain gcc 4.9 with:

.. code-block:: shell

   $ sudo apt-get install g++-4.9

Finally, set gcc 4.9 as the active compiler using:

.. code-block:: shell

   export CC=gcc-4.9
   export CXX=g++-4.9

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

Let's create a conda environment with all the C++ build and Python dependencies
from conda-forge:

On Linux and OSX:

.. code-block:: shell

    conda create -y -n pyarrow-dev -c conda-forge \
        --file arrow/ci/conda_env_unix.yml \
        --file arrow/ci/conda_env_cpp.yml \
        --file arrow/ci/conda_env_python.yml \
        python=3.6

On Windows:

.. code-block:: shell

    conda create -y -n pyarrow-dev -c conda-forge ^
        --file arrow\ci\conda_env_cpp.yml ^
        --file arrow\ci\conda_env_python.yml ^
        python=3.6

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_BUILD_TYPE=release

   export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
   export ARROW_HOME=$CONDA_PREFIX
   export PARQUET_HOME=$CONDA_PREFIX

Using pip
~~~~~~~~~

On macOS, install all dependencies through Homebrew that are required for
building Arrow C++:

.. code-block:: shell

   brew update && brew bundle --file=arrow/python/Brewfile

On Debian/Ubuntu, you need the following minimal set of dependencies. All other
dependencies will be automatically built by Arrow's third-party toolchain.

.. code-block:: shell

   $ sudo apt-get install libjemalloc-dev libboost-dev \
                          libboost-filesystem-dev \
                          libboost-system-dev \
                          libboost-regex-dev \
                          flex \
                          bison

On Arch Linux, you can get these dependencies via pacman.

.. code-block:: shell

   $ sudo pacman -S jemalloc boost

Now, let's create a Python virtualenv with all Python dependencies in the same
folder as the repositories and a target installation folder:

.. code-block:: shell

   virtualenv pyarrow
   source ./pyarrow/bin/activate
   pip install six numpy pandas cython pytest

   # This is the folder where we will install the Arrow libraries during
   # development
   mkdir dist

If your cmake version is too old on Linux, you could get a newer one via
``pip install cmake``.

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_BUILD_TYPE=release

   export ARROW_HOME=$(pwd)/dist
   export PARQUET_HOME=$(pwd)/dist
   export LD_LIBRARY_PATH=$(pwd)/dist/lib:$LD_LIBRARY_PATH

Build and test
--------------

Now build and install the Arrow C++ libraries:

.. code-block:: shell

   mkdir arrow/cpp/build
   pushd arrow/cpp/build

   cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
         -DCMAKE_INSTALL_PREFIX=$ARROW_HOME \
         -DCMAKE_INSTALL_LIBDIR=lib \
         -DARROW_PARQUET=on \
         -DARROW_PYTHON=on \
         -DARROW_PLASMA=on \
         -DARROW_BUILD_TESTS=OFF \
         ..
   make -j4
   make install
   popd

If you don't want to build and install the Plasma in-memory object store,
you can omit the ``-DARROW_PLASMA=on`` flag.

.. note::

   On Linux systems with support for building on multiple architectures,
   ``make`` may install libraries in the ``lib64`` directory by default. For
   this reason we recommend passing ``-DCMAKE_INSTALL_LIBDIR=lib`` because the
   Python build scripts assume the library directory is ``lib``

Now, build pyarrow:

.. code-block:: shell

   cd arrow/python
   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --with-parquet --with-plasma --inplace

If you did not build with plasma, you can omit ``--with-plasma``.

You should be able to run the unit tests with:

.. code-block:: shell

   $ py.test pyarrow
   ================================ test session starts ====================
   platform linux -- Python 3.6.1, pytest-3.0.7, py-1.4.33, pluggy-0.4.0
   rootdir: /home/wesm/arrow-clone/python, inifile:

   collected 1061 items / 1 skipped

   [... test output not shown here ...]

   ============================== warnings summary ===============================

   [... many warnings not shown here ...]

   ====== 1000 passed, 56 skipped, 6 xfailed, 19 warnings in 26.52 seconds =======

To build a self-contained wheel (including the Arrow and Parquet C++
libraries), one can set ``--bundle-arrow-cpp``:

.. code-block:: shell

   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --with-parquet --with-plasma --bundle-arrow-cpp bdist_wheel

Again, if you did not build with plasma, you should omit ``--with-plasma``.

Building with optional ORC integration
--------------------------------------

To build Arrow with support for the `Apache ORC file format <https://orc.apache.org/>`_,
we recommend the following:

#. Install the ORC C++ libraries and tools using ``conda``:

   .. code-block:: shell

      conda install -c conda-forge orc

#. Set ``ORC_HOME`` and ``PROTOBUF_HOME`` to the location of the installed
   Orc and protobuf C++ libraries, respectively (otherwise Arrow will try
   to download source versions of those libraries and recompile them):

   .. code-block:: shell

      export ORC_HOME=$CONDA_PREFIX
      export PROTOBUF_HOME=$CONDA_PREFIX

#. Add ``-DARROW_ORC=on`` to the CMake flags.
#. Add ``--with-orc`` to the ``setup.py`` flags.

Known issues
------------

If using packages provided by conda-forge (see "Using Conda" above)
together with a reasonably recent compiler, you may get "undefined symbol"
errors when importing pyarrow.  In that case you'll need to force the C++
ABI version to the older version used by conda-forge binaries:

.. code-block:: shell

   export CXXFLAGS="-D_GLIBCXX_USE_CXX11_ABI=0"
   export PYARROW_CXXFLAGS=$CXXFLAGS

Be sure to add ``-DCMAKE_CXX_FLAGS=$CXXFLAGS`` to the cmake invocations
when rebuilding.

Developing on Windows
=====================

First, we bootstrap a conda environment similar to the `C++ build instructions
<https://github.com/apache/arrow/blob/master/cpp/apidoc/Windows.md>`_. This
includes all the dependencies for Arrow and the Apache Parquet C++ libraries.

First, starting from fresh clones of Apache Arrow:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git

.. code-block:: shell

   conda create -y -q -n pyarrow-dev ^
         python=3.6 numpy six setuptools cython pandas pytest ^
         cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib ^
         gflags brotli lz4-c zstd -c conda-forge
   activate pyarrow-dev

Now, we build and install Arrow C++ libraries

.. code-block:: shell

   mkdir cpp\build
   cd cpp\build
   set ARROW_BUILD_TOOLCHAIN=%CONDA_PREFIX%\Library
   set ARROW_HOME=C:\thirdparty
   cmake -G "Visual Studio 14 2015 Win64" ^
         -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
         -DCMAKE_BUILD_TYPE=Release ^
         -DARROW_BUILD_TESTS=on ^
         -DARROW_CXXFLAGS="/WX /MP" ^
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
   python setup.py build_ext --inplace --with-parquet

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
