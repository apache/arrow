<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

## Developer guide for conda users

### Linux and macOS

#### System Requirements

On macOS, any modern XCode (6.4 or higher; the current version is 8.3.1) is
sufficient.

On Linux, for this guide, we recommend using gcc 4.8 or 4.9, or clang 3.7 or
higher. You can check your version by running

```shell
$ gcc --version
```

On Ubuntu 16.04 and higher, you can obtain gcc 4.9 with:

```shell
$ sudo apt-get install g++-4.9
```

Finally, set gcc 4.9 as the active compiler using:

```shell
export CC=gcc-4.9
export CXX=g++-4.9
```

#### Environment Setup and Build

First, let's create a conda environment with all the C++ build and Python
dependencies from conda-forge:

```shell
conda create -y -q -n pyarrow-dev \
      python=3.6 numpy six setuptools cython pandas pytest \
      cmake flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib \
      brotli jemalloc -c conda-forge
source activate pyarrow-dev
```

Now, let's clone the Arrow and Parquet git repositories:

```shell
mkdir repos
cd repos
git clone https://github.com/apache/arrow.git
git clone https://github.com/apache/parquet-cpp.git
```

You should now see

```shell
$ ls -l
total 8
drwxrwxr-x 12 wesm wesm 4096 Apr 15 19:19 arrow/
drwxrwxr-x 12 wesm wesm 4096 Apr 15 19:19 parquet-cpp/
```

We need to set a number of environment variables to let Arrow's build system
know about our build toolchain:

```
export ARROW_BUILD_TYPE=release

export BOOST_ROOT=$CONDA_PREFIX
export BOOST_LIBRARYDIR=$CONDA_PREFIX/lib

export FLATBUFFERS_HOME=$CONDA_PREFIX
export RAPIDJSON_HOME=$CONDA_PREFIX
export THRIFT_HOME=$CONDA_PREFIX
export ZLIB_HOME=$CONDA_PREFIX
export SNAPPY_HOME=$CONDA_PREFIX
export BROTLI_HOME=$CONDA_PREFIX
export JEMALLOC_HOME=$CONDA_PREFIX
export ARROW_HOME=$CONDA_PREFIX
export PARQUET_HOME=$CONDA_PREFIX
```

Now build and install the Arrow C++ libraries:

```shell
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
```

Now build and install the Apache Parquet libraries in your toolchain:

```shell
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
```

Now, build pyarrow:

```shell
cd arrow/python
python setup.py build_ext --build-type=$ARROW_BUILD_TYPE --with-parquet --inplace
```

You should be able to run the unit tests with:

```shell
$ py.test pyarrow
================================ test session starts ================================
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

====================== 181 passed, 17 skipped in 0.98 seconds =======================
```

### Windows

First, make sure you can [build the C++ library][1].

Now, we need to build and install the C++ libraries someplace.

```shell
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
```

After that, we must put the install directory's bin path in our `%PATH%`:

```shell
set PATH=%ARROW_HOME%\bin;%PATH%
```

Now, we can build pyarrow:

```shell
cd python
python setup.py build_ext --inplace
```

#### Running C++ unit tests with Python

Getting `python-test.exe` to run is a bit tricky because your `%PYTHONPATH%`
must be configured given the active conda environment:

```shell
set CONDA_ENV=C:\Users\wesm\Miniconda\envs\arrow-test
set PYTHONPATH=%CONDA_ENV%\Lib;%CONDA_ENV%\Lib\site-packages;%CONDA_ENV%\python35.zip;%CONDA_ENV%\DLLs;%CONDA_ENV%
```

Now `python-test.exe` or simply `ctest` (to run all tests) should work.

[1]: https://github.com/apache/arrow/blob/master/cpp/doc/Windows.md