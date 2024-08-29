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

@rem Run VsDevCmd.bat to set Visual Studio environment variables for building
@rem on the command line. This is the path for Visual Studio Enterprise 2019

call "C:\Program Files (x86)\Microsoft Visual Studio\2019\Enterprise\Common7\Tools\VsDevCmd.bat" -arch=amd64


@rem Install build dependencies with vcpkg

@rem TODO(ianmcook): change --x-manifest-root to --manifest-root after it
@rem changes in vcpkg

vcpkg install ^
    --triplet x64-windows ^
    --x-manifest-root cpp  ^
    --feature-flags=versions ^
    --clean-after-build ^
    || exit /B 1


@rem Set environment variables

set ARROW_TEST_DATA=%cd%\testing\data
set PARQUET_TEST_DATA=%cd%\cpp\submodules\parquet-testing\data


@rem Build Arrow C++ library

mkdir cpp\build
pushd cpp\build

@rem TODO(ianmcook): test using --parallel %NUMBER_OF_PROCESSORS% with
@rem cmake --build instead of specifying -DARROW_CXXFLAGS="/MP" here
@rem (see https://gitlab.kitware.com/cmake/cmake/-/issues/20564)

@rem TODO(ianmcook): Add -DARROW_BUILD_BENCHMARKS=ON after the issue described
@rem at https://github.com/google/benchmark/issues/1046 is resolved

cmake -G "Visual Studio 16 2019" -A x64 ^
      -DARROW_BOOST_USE_SHARED=ON ^
      -DARROW_BUILD_SHARED=ON ^
      -DARROW_BUILD_STATIC=OFF ^
      -DARROW_BUILD_TESTS=ON ^
      -DARROW_CXXFLAGS="/MP" ^
      -DARROW_DATASET=ON ^
      -DARROW_DEPENDENCY_SOURCE=VCPKG ^
      -DARROW_FLIGHT=OFF ^
      -DARROW_MIMALLOC=ON ^
      -DARROW_PARQUET=ON ^
      -DARROW_WITH_BROTLI=ON ^
      -DARROW_WITH_BZ2=ON ^
      -DARROW_WITH_LZ4=ON ^
      -DARROW_WITH_SNAPPY=ON ^
      -DARROW_WITH_ZLIB=ON ^
      -DARROW_WITH_ZSTD=ON ^
      -DCMAKE_BUILD_TYPE=release ^
      -DCMAKE_UNITY_BUILD=ON ^
      .. || exit /B 1

cmake --build . --target INSTALL --config Release || exit /B 1


@rem Test Arrow C++ library

ctest --output-on-failure ^
      --parallel %NUMBER_OF_PROCESSORS% ^
      --timeout 300 || exit /B 1

popd
