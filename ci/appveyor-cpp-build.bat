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

@rem In release mode, disable optimizations (/Od) for faster compiling
set CMAKE_CXX_FLAGS_RELEASE=/Od

if "%JOB%" == "Static_Crt_Build" (
  @rem Since we link the CRT statically, we should also disable building
  @rem the Arrow shared library to link the tests statically, otherwise
  @rem the Arrow DLL and the tests end up using a different instance of
  @rem the CRT, which wreaks havoc.

  @rem ARROW-5403(wesm): Since changing to using gtest DLLs we can no
  @rem longer run the unit tests because gtest.dll and the unit test
  @rem executables have different static copies of the CRT

  mkdir cpp\build-debug
  pushd cpp\build-debug

  cmake -G "%GENERATOR%" ^
        -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
        -DARROW_USE_STATIC_CRT=ON ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DARROW_BUILD_SHARED=OFF ^
        -DARROW_BUILD_TESTS=ON ^
        -DARROW_BUILD_EXAMPLES=ON ^
        -DCMAKE_BUILD_TYPE=Debug ^
        -DARROW_TEST_LINKAGE=static ^
        -DARROW_CXXFLAGS="/MP" ^
        ..  || exit /B

  cmake --build . --config Debug || exit /B
  ctest --output-on-failure -j2 || exit /B
  popd
  rmdir /S /Q cpp\build-debug

  mkdir cpp\build-release
  pushd cpp\build-release

  cmake -G "%GENERATOR%" ^
        -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
        -DARROW_USE_STATIC_CRT=ON ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DARROW_BUILD_SHARED=OFF ^
        -DARROW_BUILD_TESTS=ON ^
        -DARROW_BUILD_EXAMPLES=ON ^
        -DCMAKE_BUILD_TYPE=Release ^
        -DARROW_TEST_LINKAGE=static ^
        -DCMAKE_CXX_FLAGS_RELEASE="/MT %CMAKE_CXX_FLAGS_RELEASE%" ^
        -DARROW_CXXFLAGS="/WX /MP" ^
        ..  || exit /B

  cmake --build . --config Release || exit /B
  ctest --output-on-failure -j2 || exit /B
  popd

  @rem Finish Static_Crt_Build build successfully
  exit /B 0
)

@rem In the configurations below we disable building the Arrow static library
@rem to save some time.  Unfortunately this will still build the Parquet static
@rem library because of PARQUET-1420 (Thrift-generated symbols not exported in DLL).

if "%JOB%" == "Build_Debug" (
  mkdir cpp\build-debug
  pushd cpp\build-debug

  cmake -G "%GENERATOR%" ^
        -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
        -DARROW_BOOST_USE_SHARED=OFF ^
        -DARROW_BUILD_TESTS=ON ^
        -DARROW_BUILD_EXAMPLES=ON ^
        -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
        -DARROW_BUILD_STATIC=OFF ^
        -DARROW_CXXFLAGS="/MP" ^
        ..  || exit /B

  cmake --build . --config %CONFIGURATION% || exit /B
  ctest --output-on-failure -j2 || exit /B
  popd

  @rem Finish Debug build successfully
  exit /B 0
)

@rem Avoid Boost 1.70 because of https://github.com/boostorg/process/issues/85
set CONDA_PACKAGES=--file=ci\conda_env_python.yml ^
  python=%PYTHON% numpy=1.14 "boost-cpp<1.70"

if "%ARROW_BUILD_GANDIVA%" == "ON" (
  @rem Install llvmdev in the toolchain if building gandiva.dll
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_gandiva.yml
)

if "%JOB%" == "Toolchain" (
  @rem Install pre-built "toolchain" packages for faster builds
  set CONDA_PACKAGES=%CONDA_PACKAGES% --file=ci\conda_env_cpp.yml
)

conda create -n arrow -q -y %CONDA_PACKAGES% -c conda-forge || exit /B

call activate arrow

@rem Use Boost from Anaconda
set BOOST_ROOT=%CONDA_PREFIX%\Library
set BOOST_LIBRARYDIR=%CONDA_PREFIX%\Library\lib

call ci\cpp-msvc-build-main.bat
