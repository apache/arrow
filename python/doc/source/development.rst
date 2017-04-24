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

Developing with conda
=====================

Linux and macOS
---------------

System Requirements
~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~

First, let's create a conda environment with all the C++ build and Python
dependencies from conda-forge:

.. code-block:: shell

   conda create -y -q -n pyarrow-dev \
         python=3.6 numpy six setuptools cython pandas pytest \
         cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib \
         brotli jemalloc -c conda-forge
   source activate pyarrow-dev

Now, let's clone the Arrow and Parquet git repositories:

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

We need to set some environment variables to let Arrow's build system know
about our build toolchain:

.. code-block:: shell

   export ARROW_BUILD_TYPE=release
   export ARROW_BUILD_TOOLCHAIN=$CONDA_PREFIX
   export PARQUET_BUILD_TOOLCHAIN=$CONDA_PREFIX

Now build and install the Arrow C++ libraries:

.. code-block:: shell

   mkdir arrow/cpp/build
   pushd arrow/cpp/build

   cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
         -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
         -DARROW_PYTHON=on \
         -DARROW_BUILD_TESTS=OFF \
         ..
   make -j4
   make install
   popd

Now, optionally build and install the Apache Parquet libraries in your
toolchain:

.. code-block:: shell

   mkdir parquet-cpp/build
   pushd parquet-cpp/build

   cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
         -DCMAKE_INSTALL_PREFIX=$CONDA_PREFIX \
         -DPARQUET_BUILD_BENCHMARKS=off \
         -DPARQUET_BUILD_EXECUTABLES=off \
         -DPARQUET_ZLIB_VENDORED=off \
         -DPARQUET_BUILD_TESTS=off \
         ..

   make -j4
   make install
   popd

Now, build pyarrow:

.. code-block:: shell

   cd arrow/python
   python setup.py build_ext --build-type=$ARROW_BUILD_TYPE \
          --with-parquet --with-jemalloc --inplace

If you did not build parquet-cpp, you can omit ``--with-parquet``.

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
   pyarrow/tests/test_jemalloc.py ss
   pyarrow/tests/test_parquet.py ....................
   pyarrow/tests/test_scalars.py ..........
   pyarrow/tests/test_schema.py .........
   pyarrow/tests/test_table.py .............
   pyarrow/tests/test_tensor.py ................

   ====================== 181 passed, 17 skipped in 0.98 seconds ===========

Windows
=======

First, make sure you can `build the C++ library <https://github.com/apache/arrow/blob/master/cpp/doc/Windows.md>`_.

Now, we need to build and install the C++ libraries someplace.

.. code-block:: shell

   mkdir cpp\build
   cd cpp\build
   set ARROW_HOME=C:\thirdparty
   cmake -G "Visual Studio 14 2015 Win64" ^
         -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
         -DCMAKE_BUILD_TYPE=Release ^
         -DARROW_BUILD_TESTS=off ^
         -DARROW_PYTHON=on ..
   cmake --build . --target INSTALL --config Release
   cd ..\..

After that, we must put the install directory's bin path in our ``%PATH%``:

.. code-block:: shell

   set PATH=%ARROW_HOME%\bin;%PATH%

Now, we can build pyarrow:

.. code-block:: shell

   cd python
   python setup.py build_ext --inplace

Running C++ unit tests with Python
----------------------------------

Getting ``python-test.exe`` to run is a bit tricky because your
``%PYTHONPATH%`` must be configured given the active conda environment:

.. code-block:: shell

   set CONDA_ENV=C:\Users\wesm\Miniconda\envs\arrow-test
   set PYTHONPATH=%CONDA_ENV%\Lib;%CONDA_ENV%\Lib\site-packages;%CONDA_ENV%\python35.zip;%CONDA_ENV%\DLLs;%CONDA_ENV%

Now ``python-test.exe`` or simply ``ctest`` (to run all tests) should work.
