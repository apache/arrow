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

First, set up your thirdparty C++ toolchain using libraries from conda-forge:

```shell
conda config --add channels conda-forge

export ARROW_BUILD_TYPE=Release

export CPP_TOOLCHAIN=$HOME/cpp-toolchain
export LD_LIBRARY_PATH=$CPP_TOOLCHAIN/lib:$LD_LIBRARY_PATH

export BOOST_ROOT=$CPP_TOOLCHAIN
export FLATBUFFERS_HOME=$CPP_TOOLCHAIN
export RAPIDJSON_HOME=$CPP_TOOLCHAIN
export THRIFT_HOME=$CPP_TOOLCHAIN
export ZLIB_HOME=$CPP_TOOLCHAIN
export SNAPPY_HOME=$CPP_TOOLCHAIN
export BROTLI_HOME=$CPP_TOOLCHAIN
export JEMALLOC_HOME=$CPP_TOOLCHAIN
export ARROW_HOME=$CPP_TOOLCHAIN
export PARQUET_HOME=$CPP_TOOLCHAIN

conda create -y -q -p $CPP_TOOLCHAIN \
      flatbuffers rapidjson boost-cpp thrift-cpp snappy zlib brotli jemalloc
```

Now, activate a conda environment containing your target Python version and
NumPy installed:

```shell
conda create -y -q -n pyarrow-dev python=3.6 numpy
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

Now build and install the Arrow C++ libraries:

```shell
mkdir arrow/cpp/build
pushd arrow/cpp/build

cmake -DCMAKE_BUILD_TYPE=$ARROW_BUILD_TYPE \
      -DCMAKE_INSTALL_PREFIX=$CPP_TOOLCHAIN \
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
      -DCMAKE_INSTALL_PREFIX=$CPP_TOOLCHAIN \
      -DPARQUET_BUILD_BENCHMARKS=off \
      -DPARQUET_BUILD_EXECUTABLES=off \
      -DPARQUET_ZLIB_VENDORED=off \
      -DPARQUET_BUILD_TESTS=off \
      ..

make -j4
make install
popd
```

Now, install requisite build requirements for pyarrow, then build:

```shell
conda install -y -q six setuptools cython pandas pytest

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
