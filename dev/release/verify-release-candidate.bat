@rem Licensed to the Apache Software Foundation (ASF) under one
@rem or more contributor license agreements.  See the NOTICE file
@rem distributed with this work for additional information
@rem regarding copyright ownership.  The ASF licenses this file
@rem to you under the Apache License, Version 2.0 (the
@rem "License"); you may not use this file except in compliance
@rem with the License.  You may obtain a copy of the License at
@rem
@rem   http://www.apache.org/licenses/LICENSE-2.0
@rem
@rem Unless required by applicable law or agreed to in writing,
@rem software distributed under the License is distributed on an
@rem "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
@rem KIND, either express or implied.  See the License for the
@rem specific language governing permissions and limitations
@rem under the License.

@rem To use this script, first create the following conda environment. Change
@rem the Python version if so desired. You can also omit one or more of the
@rem libray build rem dependencies if you want to build them from source as well
@rem

@rem set PYTHON=3.6
@rem conda create -n arrow-verify-release -f -q -y python=%PYTHON%
@rem conda install -y ^
@rem       six pytest setuptools numpy pandas cython ^
@rem       thrift-cpp flatbuffers rapidjson ^
@rem       cmake ^
@rem       git ^
@rem       boost-cpp ^
@rem       snappy zlib brotli gflags lz4-c zstd || exit /B

@rem Then run from the directory containing the RC tarball
@rem
@rem verify-release-candidate.bat apache-arrow-%VERSION%

@echo on

if not exist "C:\tmp\" mkdir C:\tmp
if exist "C:\tmp\arrow-verify-release" rd C:\tmp\arrow-verify-release /s /q
if not exist "C:\tmp\arrow-verify-release" mkdir C:\tmp\arrow-verify-release

tar xvf %1.tar.gz -C "C:/tmp/"

set GENERATOR=Visual Studio 14 2015 Win64
set CONFIGURATION=release
set ARROW_SOURCE=C:\tmp\%1

pushd %ARROW_SOURCE%

call activate arrow-verify-release

set ARROW_BUILD_TOOLCHAIN=%CONDA_PREFIX%\Library
set ARROW_HOME=%CONDA_PREFIX%\Library
set PARQUET_BUILD_TOOLCHAIN=%CONDA_PREFIX%\Library
set PARQUET_HOME=%CONDA_PREFIX%\Library

@rem Build and test Arrow C++ libraries
mkdir cpp\build
pushd cpp\build

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%ARROW_HOME% ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/WX /MP" ^
      -DARROW_PYTHON=ON ^
      ..  || exit /B
cmake --build . --target INSTALL --config %CONFIGURATION%  || exit /B

@rem Needed so python-test.exe works
set PYTHONPATH=%CONDA_PREFIX%\Lib;%CONDA_PREFIX%\Lib\site-packages;%CONDA_PREFIX%\python35.zip;%CONDA_PREFIX%\DLLs;%CONDA_PREFIX%;%PYTHONPATH%

ctest -VV  || exit /B
popd

@rem Build parquet-cpp
git clone https://github.com/apache/parquet-cpp.git || exit /B
mkdir parquet-cpp\build
pushd parquet-cpp\build

cmake -G "%GENERATOR%" ^
     -DCMAKE_INSTALL_PREFIX=%PARQUET_HOME% ^
     -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
     -DPARQUET_BOOST_USE_SHARED=OFF ^
     -DPARQUET_BUILD_TESTS=off .. || exit /B
cmake --build . --target INSTALL --config %CONFIGURATION% || exit /B
popd

@rem Build and import pyarrow
@rem parquet-cpp has some additional runtime dependencies that we need to figure out
@rem see PARQUET-1018
pushd python

set PYARROW_CXXFLAGS=/WX
python setup.py build_ext --inplace --with-parquet --bundle-arrow-cpp bdist_wheel  || exit /B
py.test pyarrow -v -s --parquet || exit /B

popd
