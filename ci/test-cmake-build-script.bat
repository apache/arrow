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

conda update --yes --quiet conda
conda create -n arrow-cmake-script-tests -q -y
conda install -n arrow-cmake-script-tests -q -y -c conda-forge ^
cmake git boost-cpp
call activate arrow-cmake-script-tests

mkdir cpp\build-cmake-test
pushd cpp\build-cmake-test

echo Test cmake script errors out on flatbuffers missed
set FLATBUFFERS_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"Could not find the Flatbuffers library" error.txt || exit /B
set FLATBUFFERS_HOME=

echo Test cmake script errors out on gflags missed
set GFLAGS_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"No static or shared library provided for gflags" error.txt || exit /B
set GFLAGS_HOME=

echo Test cmake script errors out on snappy missed
set SNAPPY_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"Could not find the Snappy library" error.txt || exit /B
set SNAPPY_HOME=

echo Test cmake script errors out on zlib missed
set ZLIB_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"Could not find the ZLIB library" error.txt || exit /B
set ZLIB_HOME=

echo Test cmake script errors out on brotli missed
set BROTLI_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"Could not find the Brotli library" error.txt || exit /B
set BROTLI_HOME=

echo Test cmake script errors out on lz4 missed
set LZ4_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"No static or shared library provided for lz4_static" error.txt || exit /B
set LZ4_HOME=

echo Test cmake script errors out on zstd missed
set ZSTD_HOME=WrongPath

cmake -G "%GENERATOR%" ^
      -DARROW_BOOST_USE_SHARED=OFF ^
      -DCMAKE_BUILD_TYPE=%CONFIGURATION% ^
      -DARROW_CXXFLAGS="/MP" ^
      .. >nul 2>error.txt

FINDSTR /M /C:"Could NOT find ZSTD" error.txt || exit /B
set ZSTD_HOME=

popd
