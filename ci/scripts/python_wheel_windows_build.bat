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
set CMAKE_UNITY_BUILD=ON
set CMAKE_GENERATOR=Visual Studio 17 2022
set CMAKE_PLATFORM=x64
set VCPKG_ROOT=C:\vcpkg
set VCPKG_FEATURE_FLAGS=-manifests
set VCGPK_TARGET_TRIPLET=amd64-windows-static-md-%CMAKE_BUILD_TYPE%

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
    -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
    -DMSVC_LINK_VERBOSE=ON ^
    -DPARQUET_REQUIRE_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION% ^
    -DVCPKG_MANIFEST_MODE=OFF ^
    -DVCPKG_TARGET_TRIPLET=%VCGPK_TARGET_TRIPLET% ^
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
set PYARROW_CMAKE_GENERATOR=%CMAKE_GENERATOR%
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
set ARROW_HOME=C:\arrow-dist
set CMAKE_PREFIX_PATH=C:\arrow-dist

pushd C:\arrow\python

@REM Build wheel
%PYTHON_CMD% setup.py bdist_wheel || exit /B 1

@REM Repair the wheel with delvewheel
@REM
@REM Since we bundled the Arrow C++ libraries ourselves, we only need to
@REM mangle msvcp140.dll so as to avoid ABI issues when msvcp140.dll is
@REM required by multiple Python libraries in the same process.
%PYTHON_CMD% -m pip install delvewheel || exit /B 1

for /f %%i in ('dir dist\pyarrow-*.whl /B') do (set WHEEL_NAME=%cd%\dist\%%i) || exit /B 1
echo "Wheel name: %WHEEL_NAME%"

%PYTHON_CMD% -m delvewheel repair -vv ^
    --ignore-existing --with-mangle ^
    -w repaired_wheels %WHEEL_NAME% || exit /B 1

popd
