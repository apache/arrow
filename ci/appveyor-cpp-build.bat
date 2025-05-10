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

@rem Enable memory debug checks if the env is not set already
IF "%ARROW_DEBUG_MEMORY_POOL%"=="" (
  set ARROW_DEBUG_MEMORY_POOL=trap
)

set CMAKE_BUILD_PARALLEL_LEVEL=%NUMBER_OF_PROCESSORS%
set CTEST_PARALLEL_LEVEL=%NUMBER_OF_PROCESSORS%

call activate arrow

@rem The "main" C++ build script for Windows CI
@rem (i.e. for usual configurations)

set ARROW_CMAKE_ARGS=-DARROW_DEPENDENCY_SOURCE=CONDA -DARROW_WITH_BZ2=ON

@rem Enable warnings-as-errors
set ARROW_CXXFLAGS=/WX /MP

@rem Install GCS testbench
set PIPX_BIN_DIR=C:\Windows\
call %CD%\ci\scripts\install_gcs_testbench.bat
storage-testbench -h || exit /B

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

cmake -G "%GENERATOR%" %ARROW_CMAKE_ARGS% ^
      -DARROW_ACERO=ON ^
      -DARROW_BOOST_USE_SHARED=ON ^
      -DARROW_BUILD_EXAMPLES=ON ^
      -DARROW_BUILD_STATIC=OFF ^
      -DARROW_BUILD_TESTS=ON ^
      -DARROW_COMPUTE=ON ^
      -DARROW_CSV=ON ^
      -DARROW_CXXFLAGS="%ARROW_CXXFLAGS%" ^
      -DARROW_DATASET=ON ^
      -DARROW_ENABLE_TIMING_TESTS=OFF ^
      -DARROW_FILESYSTEM=ON ^
      -DARROW_FLIGHT=%ARROW_BUILD_FLIGHT% ^
      -DARROW_FLIGHT_SQL=%ARROW_BUILD_FLIGHT_SQL% ^
      -DARROW_GANDIVA=%ARROW_BUILD_GANDIVA% ^
      -DARROW_GCS=%ARROW_GCS% ^
      -DARROW_HDFS=ON ^
      -DARROW_JSON=ON ^
      -DARROW_MIMALLOC=ON ^
      -DARROW_ORC=%ARROW_ORC% ^
      -DARROW_PARQUET=ON ^
      -DARROW_S3=%ARROW_S3% ^
      -DARROW_SUBSTRAIT=ON ^
      -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
      -DARROW_WITH_BROTLI=ON ^
      -DARROW_WITH_LZ4=ON ^
      -DARROW_WITH_SNAPPY=ON ^
      -DARROW_WITH_ZLIB=ON ^
      -DARROW_WITH_ZSTD=ON ^
      -DCMAKE_BUILD_TYPE="Release" ^
      -DCMAKE_CXX_FLAGS_RELEASE="/MD /Od /UNDEBUG" ^
      -DCMAKE_CXX_STANDARD=17 ^
      -DCMAKE_INSTALL_PREFIX=%CONDA_PREFIX%\Library ^
      -DCMAKE_UNITY_BUILD=ON ^
      -DCMAKE_VERBOSE_MAKEFILE=OFF ^
      -DPARQUET_BUILD_EXECUTABLES=ON ^
      -DPARQUET_REQUIRE_ENCRYPTION=ON ^
      ..  || exit /B
cmake --build . --target install --config Release || exit /B

@rem For ORC C++
set TZDIR=%CONDA_PREFIX%\share\zoneinfo

@rem For finding Python executable for GCS tests
set PYTHON=python

ctest --output-on-failure || exit /B

popd

pushd python

@rem
@rem Build and install pyarrow
@rem

set PYARROW_WITH_ACERO=enabled
set PYARROW_WITH_DATASET=enabled

if /i "%ARROW_BUILD_FLIGHT%" == "ON" (
    set PYARROW_WITH_FLIGHT=enabled
) else (
    set PYARROW_WITH_FLIGHT=auto
)

if /i "%ARROW_BUILD_GANDIVA%" == "ON" (
    set PYARROW_WITH_GANDIVA=enabled
) else (
    set PYARROW_WITH_GANDIVA=auto
)

if /i "%ARROW_BUILD_GCS%" == "ON" (
    set PYARROW_WITH_GCS=enabled
) else (
    set PYARROW_WITH_GCS=auto
)

if /i "%ARROW_BUILD_ORC%" == "ON" (
    set PYARROW_WITH_ORC=enabled
) else (
    set PYARROW_WITH_ORC=auto
)

set PYARROW_WITH_PARQUET=enabled
set PYARROW_WITH_PARQUET_ENCRYPTION=enabled

if /i "%ARROW_BUILD_S3%" == "ON" (
    set PYARROW_WITH_S3=enabled
) else (
    set PYARROW_WITH_S3=auto
)

set PYARROW_WITH_SUBSTRAIT=enabled

set ARROW_HOME=%CONDA_PREFIX%\Library
@rem ARROW-3075; pkgconfig is broken for Parquet for now
set PARQUET_HOME=%CONDA_PREFIX%\Library

pip install --no-deps --no-build-isolation -vv . ^
    -Csetup-args="-Dacero=%PYARROW_WITH_ACERO%" ^
    -Csetup-args="-Ddataset=%PYARROW_WITH_DATASET%" ^
    -Csetup-args="-Dflight=%PYARROW_WITH_FLIGHT%" ^
    -Csetup-args="-Dgandiva=%PYARROW_WITH_GANDIVA%" ^
    -Csetup-args="-Dgcs=%PYARROW_WITH_GCS%" ^
    -Csetup-args="-Dorc=%PYARROW_WITH_ORC%" ^
    -Csetup-args="-Dparquet=%PYARROW_WITH_PARQUET%" ^
    -Csetup-args="-Dparquet_encryption=%PYARROW_WITH_PARQUET_ENCRYPTION%" ^
    -Csetup-args="-Ds3=%PYARROW_WITH_S3%" ^
    -Csetup-args="-Dsubstrait=%PYARROW_WITH_SUBSTRAIT%"

@rem
@rem Run pyarrow tests
@rem

@rem Download IANA Timezone Database to a non-standard location to
@rem test the configurability of the timezone database path
curl https://data.iana.org/time-zones/releases/tzdata2024b.tar.gz --output tzdata.tar.gz || exit /B
mkdir %USERPROFILE%\Downloads\test\tzdata
tar --extract --file tzdata.tar.gz --directory %USERPROFILE%\Downloads\test\tzdata
curl https://raw.githubusercontent.com/unicode-org/cldr/master/common/supplemental/windowsZones.xml ^
  --output %USERPROFILE%\Downloads\test\tzdata\windowsZones.xml || exit /B
@rem Remove the database from the default location
rmdir /s /q %USERPROFILE%\Downloads\tzdata
@rem Set the env var for the non-standard location of the database
@rem (only needed for testing purposes)
set PYARROW_TZDATA_PATH=%USERPROFILE%\Downloads\test\tzdata

set AWS_EC2_METADATA_DISABLED=true
set PYTHONDEVMODE=1

popd

python -m pytest -r sxX --durations=15 --pyargs pyarrow || exit /B
