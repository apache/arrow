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

git config core.symlinks true
git reset --hard

@rem Retrieve git submodules, configure env var for Parquet unit tests
git submodule update --init || exit /B

set ARROW_TEST_DATA=%CD%\testing\data
set PARQUET_TEST_DATA=%CD%\cpp\submodules\parquet-testing\data

set ARROW_DEBUG_MEMORY_POOL=trap

@rem
@rem In the configurations below we disable building the Arrow static library
@rem to save some time.  Unfortunately this will still build the Parquet static
@rem library because of PARQUET-1420 (Thrift-generated symbols not exported in DLL).
@rem
if "%JOB%" == "Build_Debug" (
  mkdir cpp\build-debug
  pushd cpp\build-debug

  cmake -G "%GENERATOR%" ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DARROW_BUILD_EXAMPLES=ON ^
        -DARROW_BUILD_STATIC=OFF ^
        -DARROW_BUILD_TESTS=ON ^
        -DARROW_CXXFLAGS="/MP" ^
        -DARROW_ENABLE_TIMING_TESTS=OFF ^
        -DARROW_USE_PRECOMPILED_HEADERS=OFF ^
        -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
        -DCMAKE_BUILD_TYPE="Debug" ^
        -DCMAKE_UNITY_BUILD=ON ^
        .. || exit /B

  cmake --build . --config Debug || exit /B
  ctest --output-on-failure -j2 || exit /B
  popd

  @rem Finish Debug build successfully
  exit /B 0
)

call activate arrow

@rem Use Boost from Anaconda
set BOOST_ROOT=%CONDA_PREFIX%\Library
set BOOST_LIBRARYDIR=%CONDA_PREFIX%\Library\lib

@rem The "main" C++ build script for Windows CI
@rem (i.e. for usual configurations)

if "%JOB%" == "Toolchain" (
  set CMAKE_ARGS=-DARROW_DEPENDENCY_SOURCE=CONDA -DARROW_WITH_BZ2=ON
) else (
  @rem We're in a conda environment but don't want to use it for the dependencies
  set CMAKE_ARGS=-DARROW_DEPENDENCY_SOURCE=AUTO
)

@rem Enable warnings-as-errors
set ARROW_CXXFLAGS=/WX /MP

@rem
@rem Build and test Arrow C++ libraries (including Parquet)
@rem

mkdir cpp\build
pushd cpp\build

@rem XXX Without forcing CMAKE_CXX_COMPILER, CMake can re-run itself and
@rem unfortunately switch from Release to Debug mode...
@rem
@rem In release mode, disable optimizations (/Od) for faster compiling
@rem and enable runtime assertions.

cmake -G "%GENERATOR%" %CMAKE_ARGS% ^
      -DARROW_BOOST_USE_SHARED=ON ^
      -DARROW_BUILD_EXAMPLES=ON ^
      -DARROW_BUILD_STATIC=OFF ^
      -DARROW_BUILD_TESTS=ON ^
      -DARROW_CSV=ON ^
      -DARROW_CXXFLAGS="%ARROW_CXXFLAGS%" ^
      -DARROW_DATASET=ON ^
      -DARROW_ENABLE_TIMING_TESTS=OFF ^
      -DARROW_ENGINE=ON ^
      -DARROW_FLIGHT=%ARROW_BUILD_FLIGHT% ^
      -DARROW_GANDIVA=%ARROW_BUILD_GANDIVA% ^
      -DARROW_MIMALLOC=ON ^
      -DARROW_PARQUET=ON ^
      -DARROW_PYTHON=ON ^
      -DARROW_S3=%ARROW_S3% ^
      -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
      -DARROW_WITH_BROTLI=ON ^
      -DARROW_WITH_LZ4=ON ^
      -DARROW_WITH_SNAPPY=ON ^
      -DARROW_WITH_ZLIB=ON ^
      -DARROW_WITH_ZSTD=ON ^
      -DCMAKE_BUILD_TYPE="Release" ^
      -DCMAKE_CXX_COMPILER=clcache ^
      -DCMAKE_CXX_FLAGS_RELEASE="/MD /Od /UNDEBUG" ^
      -DCMAKE_INSTALL_PREFIX=%CONDA_PREFIX%\Library ^
      -DCMAKE_UNITY_BUILD=ON ^
      -DCMAKE_VERBOSE_MAKEFILE=OFF ^
      -DPARQUET_BUILD_EXECUTABLES=ON ^
      -DPARQUET_REQUIRE_ENCRYPTION=ON ^
      ..  || exit /B
cmake --build . --target install --config %CONFIGURATION%  || exit /B

@rem Needed so arrow-python-test.exe works
set OLD_PYTHONHOME=%PYTHONHOME%
set PYTHONHOME=%CONDA_PREFIX%

ctest --output-on-failure -j2 || exit /B

set PYTHONHOME=%OLD_PYTHONHOME%
popd

@rem
@rem Build and install pyarrow
@rem

pushd python

set PYARROW_BUNDLE_BOOST=OFF
set PYARROW_CMAKE_GENERATOR=%GENERATOR%
set PYARROW_CXXFLAGS=%ARROW_CXXFLAGS%
set PYARROW_PARALLEL=2
set PYARROW_WITH_DATASET=ON
set PYARROW_WITH_FLIGHT=%ARROW_BUILD_FLIGHT%
set PYARROW_WITH_GANDIVA=%ARROW_BUILD_GANDIVA%
set PYARROW_WITH_PARQUET=ON
set PYARROW_WITH_S3=%ARROW_S3%
set PYARROW_WITH_STATIC_BOOST=ON

set ARROW_HOME=%CONDA_PREFIX%\Library
@rem ARROW-3075; pkgconfig is broken for Parquet for now
set PARQUET_HOME=%CONDA_PREFIX%\Library

python setup.py develop -q || exit /B

set PYTHONDEVMODE=1

py.test -r sxX --durations=15 --pyargs pyarrow.tests || exit /B

@rem
@rem Wheels are built and tested separately (see ARROW-5142).
@rem

