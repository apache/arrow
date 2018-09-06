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

@echo on

if "%JOB%" == "Static_Crt_Build" (
  mkdir cpp\build-debug
  pushd cpp\build-debug

  cmake -G "%GENERATOR%" ^
        -DARROW_USE_STATIC_CRT=ON ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DCMAKE_BUILD_TYPE=Debug ^
        -DARROW_CXXFLAGS="/MP" ^
        ..  || exit /B

  cmake --build . --config Debug || exit /B
  ctest -VV  || exit /B
  popd

  mkdir cpp\build-release
  pushd cpp\build-release

  cmake -G "%GENERATOR%" ^
        -DARROW_USE_STATIC_CRT=ON ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DCMAKE_BUILD_TYPE=Release ^
        -DARROW_CXXFLAGS="/WX /MP" ^
        ..  || exit /B

  cmake --build . --config Release || exit /B
  ctest -VV  || exit /B
  popd

  @rem Finish Static_Crt_Build build successfully
  exit /B 0
)

if "%JOB%" == "Build_Debug" (
  mkdir cpp\build-debug
  pushd cpp\build-debug

  cmake -G "%GENERATOR%" ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
        -DARROW_CXXFLAGS="/MP" ^
        ..  || exit /B

  cmake --build . --config Debug || exit /B
  ctest -VV  || exit /B
  popd

  @rem Finish Debug build successfully
  exit /B 0
)

conda create -n arrow -q -y -c conda-forge ^
      python=%PYTHON% ^
      six pytest setuptools numpy pandas cython ^
      thrift-cpp=0.11.0 boost-cpp

call activate arrow

@rem Use Boost from conda-forge
set BOOST_ROOT=%CONDA_PREFIX%\Library
set BOOST_LIBRARYDIR=%CONDA_PREFIX%\Library\lib

if "%JOB%" == "Toolchain" (
  @rem Install pre-built "toolchain" packages for faster builds
  conda install -q -y -c conda-forge ^
      brotli ^
      cmake ^
      flatbuffers ^
      gflags ^
      gtest ^
      git ^
      lz4-c ^
      rapidjson ^
      snappy ^
      zlib ^
      zstd
  set ARROW_BUILD_TOOLCHAIN=%CONDA_PREFIX%\Library
)

set ARROW_HOME=%CONDA_PREFIX%\Library

@rem Retrieve git submodules, configure env var for Parquet unit tests
git submodule update --init
set PARQUET_TEST_DATA=%CD%\cpp\submodules\parquet-testing\data

@rem Build and test Arrow C++ libraries

mkdir cpp\build
pushd cpp\build

cmake -G "%GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX=%CONDA_PREFIX%\Library ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/WX /MP" ^
      -DARROW_PARQUET=ON ^
      -DARROW_PYTHON=ON ^
      ..  || exit /B
cmake --build . --target install --config %CONFIGURATION%  || exit /B

@rem Needed so python-test.exe works
set OLD_PYTHONHOME=%PYTHONHOME%
set PYTHONHOME=%CONDA_PREFIX%

ctest -VV  || exit /B

set PYTHONHOME=%OLD_PYTHONHOME%
popd

@rem Build and install pyarrow

pushd python

pip install pickle5

set PYARROW_CXXFLAGS=/WX
set PYARROW_CMAKE_GENERATOR=%GENERATOR%
set PYARROW_BUNDLE_ARROW_CPP=ON
set PYARROW_BUNDLE_BOOST=OFF
set PYARROW_WITH_STATIC_BOOST=ON
set PYARROW_WITH_PARQUET=ON

@rem ARROW-3075; pkgconfig is broken for Parquet for now
set PARQUET_HOME=%CONDA_PREFIX%\Library

python setup.py build_ext ^
    install -q --single-version-externally-managed --record=record.text ^
    bdist_wheel -q || exit /B

for /F %%i in ('dir /B /S dist\*.whl') do set WHEEL_PATH=%%i

@rem Test directly from installed location

@rem Needed for test_cython
SET PYARROW_PATH=%CONDA_PREFIX%\Lib\site-packages\pyarrow
py.test -r sxX --durations=15 -v %PYARROW_PATH% --parquet || exit /B

popd

@rem Test pyarrow wheel from pristine environment

call deactivate

conda create -n wheel_test -q -y python=%PYTHON%

call activate wheel_test

pip install %WHEEL_PATH% || exit /B

python -c "import pyarrow" || exit /B
python -c "import pyarrow.parquet" || exit /B

pip install pandas pickle5 pytest pytest-faulthandler

py.test -r sxX --durations=15 --pyargs pyarrow.tests || exit /B
