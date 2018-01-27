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

First, let's clone the Arrow and Parquet git repositories:

.. code-block:: shell

   mkdir repos
   cd repos
   git clone https://github.com/apache/arrow.git
   git clone https://github.com/apache/parquet-cpp.git

You should now see


.. code-block:: shell

   $ ls -l
   total 8
   drwxrwxr-x 12 wesm wesm 4096 Apr 15 19:19 arrow/
   drwxrwxr-x 12 wesm wesm 4096 Apr 15 19:19 parquet-cpp/

Using Conda
~~~~~~~~~~~

Let's create a conda environment with all the C++ build and Python dependencies
from conda-forge:

.. code-block:: shell

   conda create -y -q -n pyarrow-dev \
         python=3.6 numpy six setuptools cython pandas pytest \
         cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib \
         gflags brotli jemalloc lz4-c zstd -c conda-forge
   source activate pyarrow-dev


We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_BUILD_TYPE=release

   export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
   export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX
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
                          libboost-system-dev

On Arch Linux, you can get these dependencies via pacman.

.. code-block:: shell

   $ sudo pacman -S jemalloc boost

Now, let's create a Python virtualenv with all Python dependencies in the same
folder as the repositories and a target installation folder:

.. code-block:: shell

   virtualenv pyarrow
   source ./pyarrow/bin/activate
   pip install six numpy pandas cython pytest

   # This is the folder where we will install Arrow and Parquet to during
   # development
   mkdir dist

If your cmake version is too old on Linux, you could get a newer one via ``pip
install cmake``.

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
         -DARROW_PYTHON=on \
         -DARROW_PLASMA=on \
         -DARROW_BUILD_TESTS=OFF \
         ..
   make -j4
   make install
   popd

If you don't want to build and install the Plasma in-memory object store,
you can omit the ``-DARROW_PLASMA=on`` flag.

To add support for the experimental Apache ORC integration, include
``-DARROW_ORC=on`` in these flags.

Now, optionally build and install the Apache Parquet libraries in your
toolchain:

.. code-block:: shell

   mkdir parquet-cpp/build
   pushd parquet-cpp/build

   cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
         -DCMAKE_INSTALL_PREFIX=$PARQUET_HOME \
         -DPARQUET_BUILD_BENCHMARKS=off \
         -DPARQUET_BUILD_EXECUTABLES=off \
         -DPARQUET_BUILD_TESTS=off \
         ..

   make -j4
   make install
   popd

Now, build pyarrow:

.. code-block:: shell

   cd arrow/python
   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --with-parquet --with-plasma --inplace

If you did not build parquet-cpp, you can omit ``--with-parquet`` and if
you did not build with plasma, you can omit ``--with-plasma``.

If you built with the experimental Apache ORC integration, include
``--with-orc`` in these flags.

You should be able to run the unit tests with:

.. code-block:: shell

   $ py.test pyarrow
   ================================ test session starts ====================
   platform linux -- Python 3.6.1, pytest-3.0.7, py-1.4.33, pluggy-0.4.0
   rootdir: /home/wesm/arrow-clone/python, inifile:
   collected 198 items

   pyarrow/tests/test_array.py ...........
   pyarrow/tests/test_convert_builtin.py .....................
   pyarrow/tests/test_convert_pandas.py .............................
   pyarrow/tests/test_feather.py ..........................
   pyarrow/tests/test_hdfs.py sssssssssssssss
   pyarrow/tests/test_io.py ..................
   pyarrow/tests/test_ipc.py ........
   pyarrow/tests/test_parquet.py ....................
   pyarrow/tests/test_scalars.py ..........
   pyarrow/tests/test_schema.py .........
   pyarrow/tests/test_table.py .............
   pyarrow/tests/test_tensor.py ................

   ====================== 181 passed, 17 skipped in 0.98 seconds ===========

To build a self-contained wheel (include Arrow C++ and Parquet C++), one can set `--bundle-arrow-cpp`:

.. code-block:: shell

   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --with-parquet --with-plasma --bundle-arrow-cpp bdist_wheel

Again, if you did not build parquet-cpp, you should omit ``--with-parquet`` and
if you did not build with plasma, you should omit ``--with-plasma``.

Developing on Windows
=====================

First, we bootstrap a conda environment similar to the `C++ build instructions
<https://github.com/apache/arrow/blob/master/cpp/apidoc/Windows.md>`_. This
includes all the dependencies for Arrow and the Apache Parquet C++ libraries.

First, starting from fresh clones of Apache Arrow and parquet-cpp:

.. code-block:: shell

   git clone https://github.com/apache/arrow.git
   git clone https://github.com/apache/parquet-cpp.git

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
         -DARROW_PYTHON=on ..
   cmake --build . --target INSTALL --config Release
   cd ..\..

Now, we build parquet-cpp and install the result in the same place:

.. code-block:: shell

   mkdir ..\parquet-cpp\build
   pushd ..\parquet-cpp\build
   set PARQUET_BUILD_TOOLCHAIN=%CONDA_PREFIX%\Library
   set PARQUET_HOME=C:\thirdparty
   cmake -G "Visual Studio 14 2015 Win64" ^
         -DCMAKE_INSTALL_PREFIX=%PARQUET_HOME% ^
         -DCMAKE_BUILD_TYPE=Release ^
         -DPARQUET_BUILD_TESTS=off ..
   cmake --build . --target INSTALL --config Release
   popd

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

Running C++ unit tests with Python
----------------------------------

Getting ``python-test.exe`` to run is a bit tricky because your
``%PYTHONPATH%`` must be configured given the active conda environment:

.. code-block:: shell

   set CONDA_ENV=C:\Users\wesm\Miniconda\envs\arrow-test
   set PYTHONPATH=%CONDA_ENV%\Lib;%CONDA_ENV%\Lib\site-packages;%CONDA_ENV%\python35.zip;%CONDA_ENV%\DLLs;%CONDA_ENV%

Now ``python-test.exe`` or simply ``ctest`` (to run all tests) should work.

Nightly Builds of `arrow-cpp`, `parquet-cpp`, and `pyarrow` for Linux
---------------------------------------------------------------------

Nightly builds of Linux conda packages for ``arrow-cpp``, ``parquet-cpp``, and
``pyarrow`` can be automated using an open source tool called `scourge
<https://github.com/cpcloud/scourge>`_.

``scourge`` is new, so please report any feature requests or bugs to the
`scourge issue tracker <https://github.com/cpcloud/scourge/issues>`_.

To get scourge you need to clone the source and install it in development mode.

To setup your own nightly builds:

#. Clone and install scourge
#. Create a script that calls scourge
#. Run that script as a cronjob once per day

First, clone and install scourge (you also need to `install docker
<https://docs.docker.com/engine/installation>`):


.. code:: sh

   git clone https://github.com/cpcloud/scourge
   cd scourge
   python setup.py develop
   which scourge

Second, create a shell script that calls scourge:

.. code:: sh

   function build() {
     # make sure we got a working directory
     workingdir="${1}"
     [ -z "${workingdir}" ] && echo "Must provide a working directory" && exit 1
     scourge="/path/to/scourge"

     # get the hash of master for building parquet
     PARQUET_ARROW_VERSION="$("${scourge}" sha apache/arrow master)"

     # setup the build for each package
     "${scourge}" init arrow-cpp@master parquet-cpp@master pyarrow@master

     # build the packages with some constraints (the -c arguments)
     # -e sets environment variables on a per package basis
     "${scourge}" build \
       -e parquet-cpp:PARQUET_ARROW_VERSION="${PARQUET_ARROW_VERSION}" \
       -c "python >=2.7,<3|>=3.5" \
       -c "numpy >= 1.11" \
       -c "r-base >=3.3.2"
   }

   workingdir="$(date +'%Y%m%d_%H_%M_%S')"
   mkdir -p "${workingdir}"
   build "${workingdir}" > "${workingdir}"/scourge.log 2>&1

Third, run that script as a cronjob once per day:

.. code:: sh

   crontab -e

then in the scratch file that's opened:

.. code:: sh

   @daily /path/to/the/above/script.sh

The build artifacts (conda packages) will be located in
``${workingdir}/artifacts/linux-64``.
