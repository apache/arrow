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

echo "Building windows wheel..."

@REM List installed Pythons
py -0p

%PYTHON_CMD% -m sysconfig || exit /B 1

call "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Auxiliary\Build\vcvars64.bat"
@echo on

echo "=== (%PYTHON%) Clear output directories and leftovers ==="
del /s /q C:\arrow-build
del /s /q C:\arrow-dist
del /s /q C:\arrow\python\dist
del /s /q C:\arrow\python\build
del /s /q C:\arrow\python\pyarrow\*.so
del /s /q C:\arrow\python\pyarrow\*.so.*

echo "=== (%PYTHON%) Building Arrow C++ libraries ==="
set ARROW_ACERO=ON
set ARROW_DATASET=ON
set ARROW_FLIGHT=ON
set ARROW_GANDIVA=OFF
set ARROW_GCS=ON
set ARROW_HDFS=ON
set ARROW_ORC=ON
set ARROW_PARQUET=ON
set PARQUET_REQUIRE_ENCRYPTION=ON
set ARROW_MIMALLOC=ON
set ARROW_SUBSTRAIT=ON
set ARROW_S3=ON
set ARROW_TENSORFLOW=ON
set ARROW_WITH_BROTLI=ON
set ARROW_WITH_BZ2=ON
set ARROW_WITH_LZ4=ON
set ARROW_WITH_SNAPPY=ON
set ARROW_WITH_ZLIB=ON
set ARROW_WITH_ZSTD=ON
set CMAKE_INTERPROCEDURAL_OPTIMIZATION=ON
set CMAKE_UNITY_BUILD=ON
set CMAKE_GENERATOR=Visual Studio 17 2022
set CMAKE_PLATFORM=x64
set VCPKG_ROOT=C:\vcpkg
set VCPKG_FEATURE_FLAGS=-manifests
set VCPKG_TARGET_TRIPLET=amd64-windows-static-md-%CMAKE_BUILD_TYPE%

mkdir C:\arrow-build
pushd C:\arrow-build
cmake ^
    -DARROW_ACERO=%ARROW_ACERO% ^
    -DARROW_BUILD_SHARED=ON ^
    -DARROW_BUILD_STATIC=OFF ^
    -DARROW_BUILD_TESTS=OFF ^
    -DARROW_COMPUTE=ON ^
    -DARROW_CSV=ON ^
    -DARROW_CXXFLAGS="/MP" ^
    -DARROW_DATASET=%ARROW_DATASET% ^
    -DARROW_DEPENDENCY_SOURCE=VCPKG ^
    -DARROW_DEPENDENCY_USE_SHARED=OFF ^
    -DARROW_FILESYSTEM=ON ^
    -DARROW_FLIGHT=%ARROW_FLIGHT% ^
    -DARROW_GANDIVA=%ARROW_GANDIVA% ^
    -DARROW_GCS=%ARROW_GCS% ^
    -DARROW_HDFS=%ARROW_HDFS% ^
    -DARROW_JSON=ON ^
    -DARROW_MIMALLOC=%ARROW_MIMALLOC% ^
    -DARROW_ORC=%ARROW_ORC% ^
    -DARROW_PACKAGE_KIND="python-wheel-windows" ^
    -DARROW_PARQUET=%ARROW_PARQUET% ^
    -DARROW_S3=%ARROW_S3% ^
    -DARROW_SUBSTRAIT=%ARROW_SUBSTRAIT% ^
    -DARROW_TENSORFLOW=%ARROW_TENSORFLOW% ^
    -DARROW_WITH_BROTLI=%ARROW_WITH_BROTLI% ^
    -DARROW_WITH_BZ2=%ARROW_WITH_BZ2% ^
    -DARROW_WITH_LZ4=%ARROW_WITH_LZ4% ^
    -DARROW_WITH_SNAPPY=%ARROW_WITH_SNAPPY% ^
    -DARROW_WITH_ZLIB=%ARROW_WITH_ZLIB% ^
    -DARROW_WITH_ZSTD=%ARROW_WITH_ZSTD% ^
    -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
    -DCMAKE_INSTALL_PREFIX=C:\arrow-dist ^
    -DCMAKE_INTERPROCEDURAL_OPTIMIZATION=%CMAKE_INTERPROCEDURAL_OPTIMIZATION% ^
    -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
    -DMSVC_LINK_VERBOSE=ON ^
    -DPARQUET_REQUIRE_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION% ^
    -DVCPKG_MANIFEST_MODE=OFF ^
    -DVCPKG_TARGET_TRIPLET=%VCPKG_TARGET_TRIPLET% ^
    -Dxsimd_SOURCE=BUNDLED ^
    -G "%CMAKE_GENERATOR%" ^
    -A "%CMAKE_PLATFORM%" ^
    C:\arrow\cpp || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install || exit /B 1
popd

echo "=== (%PYTHON%) Building wheel ==="
set PYARROW_BUILD_TYPE=%CMAKE_BUILD_TYPE%
set PYARROW_BUILD_VERBOSE=1
set PYARROW_BUNDLE_ARROW_CPP=ON

if %ARROW_ACERO% == ON (
    set PYARROW_WITH_ACERO=enabled
) else if %ARROW_ACERO% == OFF (
    set PYARROW_WITH_ACERO=disabled
) else (
    set PYARROW_WITH_ACERO=auto
)
if %ARROW_DATASET% == ON (
    set PYARROW_WITH_DATASET=enabled
) else if %ARROW_DATASET% == OFF (
    set PYARROW_WITH_DATASET=disabled
) else (
    set PYARROW_WITH_DATASET=auto
)
if %ARROW_FLIGHT% == ON (
    set PYARROW_WITH_FLIGHT=enabled
) else if %ARROW_FLIGHT% == OFF (
    set PYARROW_WITH_FLIGHT=disabled
) else (
    set PYARROW_WITH_FLIGHT=auto
)
if %ARROW_GANDIVA% == ON (
    set PYARROW_WITH_GANDIVA=enabled
) else if %ARROW_GANDIVA% == OFF (
    set PYARROW_WITH_GANDIVA=disabled
) else (
    set PYARROW_WITH_GANDIVA=auto
)
if %ARROW_GCS% == ON (
    set PYARROW_WITH_GCS=enabled
) else if %ARROW_GCS% == OFF (
    set PYARROW_WITH_GCS=disabled
) else (
    set PYARROW_WITH_GCS=auto
)
if %ARROW_HDFS% == ON (
    set PYARROW_WITH_HDFS=enabled
) else if %ARROW_HDFS% == OFF (
    set PYARROW_WITH_HDFS=disabled
) else (
    set PYARROW_WITH_HDFS=auto
)
if %ARROW_ORC% == ON (
    set PYARROW_WITH_ORC=enabled
) else if %ARROW_ORC% == OFF (
    set PYARROW_WITH_ORC=disabled
) else (
    set PYARROW_WITH_ORC=auto
)
if %ARROW_PARQUET% == ON (
    set PYARROW_WITH_PARQUET=enabled
) else if %ARROW_PARQUET% == OFF (
    set PYARROW_WITH_PARQUET=disabled
) else (
    set PYARROW_WITH_PARQUET=auto
)
if %PARQUET_REQUIRE_ENCRYPTION% == ON (
    set PYARROW_WITH_PARQUET_ENCRYPTION=enabled
) else if %PARQUET_REQUIRE_ENCRYPTION% == OFF (
    set PYARROW_WITH_PARQUET_ENCRYPTION=disabled
) else (
    set PYARROW_WITH_PARQUET_ENCRYPTION=auto
)
if %ARROW_SUBSTRAIT% == ON (
    set PYARROW_WITH_SUBSTRAIT=enabled
) else if %ARROW_SUBSTRAIT% == OFF (
    set PYARROW_WITH_SUBSTRAIT=disabled
) else (
    set PYARROW_WITH_SUBSTRAIT=auto
)
if %ARROW_S3% == ON (
    set PYARROW_WITH_S3=enabled
) else if %ARROW_S3% == OFF (
    set PYARROW_WITH_S3=disabled
) else (
    set PYARROW_WITH_S3=auto
)

@REM Meson sdist requires setuptools_scm to be able to get the version from git
git config --global --add safe.directory C:\arrow

pushd C:\arrow\python

@REM TODO: Remove once docker rebuild works correctly
@REM See: https://github.com/apache/arrow/issues/48947
%PYTHON_CMD% -m pip install -U build  || exit /B 1

@REM Build wheel
%PYTHON_CMD% -m build --sdist --wheel . ^
    -Csetup-args="-Dbuildtype=%CMAKE_BUILD_TYPE%" ^
    -Csetup-args="-Dacero=%PYARROW_WITH_ACERO%" ^
    -Csetup-args="-Ddataset=%PYARROW_WITH_DATASET%" ^
    -Csetup-args="-Dflight=%PYARROW_WITH_FLIGHT%" ^
    -Csetup-args="-Dgandiva=%PYARROW_WITH_GANDIVA%" ^
    -Csetup-args="-Dgcs=%PYARROW_WITH_GCS%" ^
    -Csetup-args="-Dhdfs=%PYARROW_WITH_HDFS%" ^
    -Csetup-args="-Dorc=%PYARROW_WITH_ORC%" ^
    -Csetup-args="-Dparquet=%PYARROW_WITH_PARQUET%" ^
    -Csetup-args="-Dparquet_require_encryption=%PYARROW_WITH_PARQUET_ENCRYPTION%" ^
    -Csetup-args="-Dsubstrait=%PYARROW_WITH_SUBSTRAIT%" ^
    -Csetup-args="-Ds3=%PYARROW_WITH_S3%" ^
    -Csetup-args="--cmake-prefix-path=C:\arrow-dist" || exit /B 1

@REM Repair the wheel with delvewheel
@REM
@REM Since we bundled the Arrow C++ libraries ourselves, we only need to
@REM mangle msvcp140.dll so as to avoid ABI issues when msvcp140.dll is
@REM required by multiple Python libraries in the same process.
%PYTHON_CMD% -m pip install delvewheel || exit /B 1

@REM Copy .lib files to bin directory for delvewheel to find them
copy C:\arrow-dist\lib\*.lib C:\arrow-dist\bin\ || exit /B 1

for /f %%i in ('dir dist\pyarrow-*.whl /B') do (set WHEEL_NAME=%cd%\dist\%%i) || exit /B 1
echo "Wheel name: %WHEEL_NAME%"

%PYTHON_CMD% -m delvewheel repair -vv ^
    --ignore-existing --with-mangle --include-imports ^
    --no-mangle "arrow.dll;arrow_python.dll;arrow_acero.dll;arrow_dataset.dll;arrow_flight.dll;arrow_substrait.dll;parquet.dll" ^
    --add-path "C:\arrow-dist\bin" ^
    -w repaired_wheels %WHEEL_NAME% || exit /B 1

popd
