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

call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsarm64.bat"
@echo on

echo "=== (%PYTHON%) Building Arrow C++ libraries ==="
set ARROW_SRC=%GITHUB_WORKSPACE%\arrow
set ARROW_DIST=%GITHUB_WORKSPACE%\arrow-dist
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
set ARROW_SUBSTRAIT=OFF
set ARROW_S3=ON
set ARROW_TENSORFLOW=OFF
set CMAKE_GENERATOR=Visual Studio 17 2022
set CMAKE_PLATFORM=ARM64
set VCPKG_ROOT=C:\vcpkg
set VCPKG_FEATURE_FLAGS=-manifests
set VCPKG_TARGET_TRIPLET=arm64-windows

mkdir C:\arrow-build
pushd C:\arrow-build
cmake ^
 -G "Visual Studio 17 2022" -A ARM64 ^
 -DCMAKE_BUILD_TYPE=Release ^
 -DCMAKE_TOOLCHAIN_FILE=%VCPKG_ROOT%\scripts\buildsystems\vcpkg.cmake ^
 -DVCPKG_TARGET_TRIPLET=%VCPKG_TARGET_TRIPLET% ^
 -DCMAKE_INSTALL_PREFIX=%ARROW_DIST% ^
 -DARROW_DEPENDENCY_SOURCE=VCPKG ^
 -DARROW_DEPENDENCY_USE_SHARED=ON ^
 -DARROW_PROTOBUF_USE_SHARED=ON ^
 -Dutf8proc_SOURCE=BUNDLED ^
 -DARROW_SIMD_LEVEL=NONE ^
 -DARROW_RUNTIME_SIMD_LEVEL=NONE ^
 -DARROW_USE_XSIMD=OFF ^
 -DARROW_WITH_UTF8PROC=OFF ^
 -DARROW_BUILD_SHARED=ON ^
 -DARROW_BUILD_STATIC=OFF ^
 -DARROW_BUILD_TESTS=OFF ^
 -DARROW_ACERO=%ARROW_ACERO% ^
 -DARROW_COMPUTE=ON ^
 -DARROW_CSV=ON ^
 -DARROW_DATASET=%ARROW_DATASET% ^
 -DARROW_FILESYSTEM=ON ^
 -DARROW_FLIGHT=%ARROW_FLIGHT% ^
 -DARROW_GANDIVA=%ARROW_GANDIVA% ^
 -DARROW_GCS=%ARROW_GCS% ^
 -DARROW_HDFS=%ARROW_HDFS% ^
 -DARROW_JSON=ON ^
 -DVCPKG_MANIFEST_MODE=OFF ^
 -DARROW_MIMALLOC=%ARROW_MIMALLOC% ^
 -DARROW_ORC=%ARROW_ORC% ^
 -DARROW_PACKAGE_KIND="python-wheel-windows" ^
 -DARROW_PARQUET=%ARROW_PARQUET% ^
 -DPARQUET_REQUIRE_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION% ^
 -DARROW_S3=%ARROW_S3% ^
 -DARROW_SUBSTRAIT=%ARROW_SUBSTRAIT% ^
 -DARROW_TENSORFLOW=%ARROW_TENSORFLOW% ^
 -DARROW_WITH_BROTLI=ON ^
 -DARROW_WITH_BZ2=ON ^
 -DARROW_WITH_LZ4=ON ^
 -DARROW_WITH_SNAPPY=ON ^
 -DARROW_WITH_ZLIB=ON ^
 -DARROW_WITH_ZSTD=ON ^
 "%ARROW_SRC%\cpp" || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install || exit /B 1
popd

echo "=== (%PYTHON%) Building wheel ==="
set PYARROW_BUILD_TYPE=%CMAKE_BUILD_TYPE%
set CMAKE_GENERATOR=Visual Studio 17 2022
set CMAKE_PLATFORM=ARM64
set PYARROW_BUILD_VERBOSE=1
set PYARROW_BUNDLE_ARROW_CPP=ON
set PYARROW_CMAKE_GENERATOR=%CMAKE_GENERATOR%
set PYARROW_CMAKE_OPTIONS="-DCMAKE_INTERPROCEDURAL_OPTIMIZATION=%CMAKE_INTERPROCEDURAL_OPTIMIZATION%"
set PYARROW_WITH_ACERO=%ARROW_ACERO%
set PYARROW_WITH_DATASET=%ARROW_DATASET%
set PYARROW_WITH_FLIGHT=%ARROW_FLIGHT%
set PYARROW_WITH_GANDIVA=%ARROW_GANDIVA%
set PYARROW_WITH_GCS=%ARROW_GCS%
set PYARROW_WITH_HDFS=%ARROW_HDFS%
set PYARROW_WITH_ORC=%ARROW_ORC%
set PYARROW_WITH_PARQUET=%ARROW_PARQUET%
set PYARROW_WITH_PARQUET_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION%
set PYARROW_WITH_SUBSTRAIT=%ARROW_SUBSTRAIT%
set PYARROW_WITH_S3=%ARROW_S3%
set Arrow_DIR=%ARROW_DIST%\lib\cmake\arrow
set CMAKE_PREFIX_PATH=%ARROW_DIST%
set ARROW_HOME=%ARROW_DIST%

pushd %ARROW_SRC%\python

@REM Install Python dependencies
echo "=== Installing Python dependencies ==="
%PYTHON_CMD% -m pip install --upgrade pip || exit /B 1
%PYTHON_CMD% -m pip install cython numpy setuptools_scm setuptools || exit /B 1

@REM Build wheel
%PYTHON_CMD% setup.py bdist_wheel || exit /B 1

@REM Repair the wheel with delvewheel
%PYTHON_CMD% -m pip install delvewheel || exit /B 1

for /f %%i in ('dir dist\pyarrow-*.whl /B') do (set WHEEL_NAME=%cd%\dist\%%i) || exit /B 1
echo "Wheel name: %WHEEL_NAME%"

%PYTHON_CMD% -m delvewheel repair -vv --add-path "%ARROW_DIST%\bin" --add-path "C:\vcpkg\installed\arm64-windows\bin" --ignore-existing --with-mangle -w repaired_wheels %WHEEL_NAME% || exit /B 1

popd

