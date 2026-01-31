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

set SOURCE_DIR=%1
set CMAKE_INSTALL_PREFIX=%2
set CPP_SOURCE_DIR=%SOURCE_DIR%\cpp
set CPP_BUILD_DIR=%SOURCE_DIR%\build
echo C++ source dir is %CPP_SOURCE_DIR%

echo Building for Windows ...

@REM List installed Pythons
py -0p

%PYTHON_CMD% -m sysconfig || exit /B 1

@REM Setup MSVC environment

call "C:\Program Files\Microsoft Visual Studio\2022\Enterprise\VC\Auxiliary\Build\vcvarsall.bat" x64
@echo on

echo "=== CCACHE Stats before build ==="
ccache -sv

echo "=== Building Arrow C++ libraries ==="
set ARROW_ACERO=ON
set ARROW_DATASET=ON
set ARROW_FLIGHT=OFF
set ARROW_GANDIVA=OFF
set ARROW_GCS=OFF
set ARROW_HDFS=ON
set ARROW_MIMALLOC=ON
set ARROW_ORC=OFF
set ARROW_PARQUET=ON
set PARQUET_REQUIRE_ENCRYPTION=ON
set ARROW_SUBSTRAIT=ON
set ARROW_S3=ON
set ARROW_TENSORFLOW=ON
set ARROW_WITH_BROTLI=ON
set ARROW_WITH_BZ2=OFF
set ARROW_WITH_LZ4=ON
set ARROW_WITH_SNAPPY=ON
set ARROW_WITH_ZLIB=ON
set ARROW_WITH_ZSTD=ON
set CMAKE_BUILD_TYPE=Release
set CMAKE_GENERATOR=Ninja
set CMAKE_UNITY_BUILD=ON

mkdir %CPP_BUILD_DIR%
pushd %CPP_BUILD_DIR%

cmake ^
    -DARROW_ACERO=%ARROW_ACERO% ^
    -DARROW_BUILD_SHARED=ON ^
    -DARROW_BUILD_STATIC=OFF ^
    -DARROW_BUILD_TESTS=OFF ^
    -DARROW_COMPUTE=ON ^
    -DARROW_CSV=ON ^
    -DARROW_CXXFLAGS="/MP" ^
    -DARROW_DATASET=%ARROW_DATASET% ^
    -DARROW_DEPENDENCY_USE_SHARED=OFF ^
    -DARROW_FILESYSTEM=ON ^
    -DARROW_FLIGHT=%ARROW_FLIGHT% ^
    -DARROW_GANDIVA=%ARROW_GANDIVA% ^
    -DARROW_GCS=%ARROW_GCS% ^
    -DARROW_HDFS=%ARROW_HDFS% ^
    -DARROW_JSON=ON ^
    -DARROW_MIMALLOC=%ARROW_MIMALLOC% ^
    -DARROW_ORC=%ARROW_ORC% ^
    -DARROW_PARQUET=%ARROW_PARQUET% ^
    -DARROW_S3=%ARROW_S3% ^
    -DARROW_SUBSTRAIT=%ARROW_SUBSTRAIT% ^
    -DARROW_TENSORFLOW=%ARROW_TENSORFLOW% ^
    -DARROW_USE_CCACHE=ON ^
    -DARROW_WITH_BROTLI=%ARROW_WITH_BROTLI% ^
    -DARROW_WITH_BZ2=%ARROW_WITH_BZ2% ^
    -DARROW_WITH_LZ4=%ARROW_WITH_LZ4% ^
    -DARROW_WITH_SNAPPY=%ARROW_WITH_SNAPPY% ^
    -DARROW_WITH_ZLIB=%ARROW_WITH_ZLIB% ^
    -DARROW_WITH_ZSTD=%ARROW_WITH_ZSTD% ^
    -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
    -DCMAKE_INSTALL_PREFIX=%CMAKE_INSTALL_PREFIX% ^
    -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
    -DMSVC_LINK_VERBOSE=ON ^
    -DPARQUET_REQUIRE_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION% ^
    -Dxsimd_SOURCE=BUNDLED ^
    -G "%CMAKE_GENERATOR%" ^
    %CPP_SOURCE_DIR% || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install || exit /B 1
popd

echo "=== CCACHE Stats after build ==="
ccache -sv

echo "=== Building Python ==="
set PYARROW_BUILD_TYPE=%CMAKE_BUILD_TYPE%
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
if %CMAKE_BUILD_TYPE% == Release (
    set MESON_BUILD_TYPE=release
) else (
    set MESON_BUILD_TYPE=debug
)

pushd %SOURCE_DIR%\python

@REM Install Python build dependencies
%PYTHON_CMD% -m pip install --upgrade pip || exit /B 1
%PYTHON_CMD% -m pip install -r requirements-build.txt || exit /B 1
%PYTHON_CMD% -m pip install build || exit /B 1

@REM Build PyArrow
%PYTHON_CMD% -m build --wheel --no-isolation . ^
    -Csetup-args="-Dbuildtype=%MESON_BUILD_TYPE%" ^
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
    -Csetup-args="-Ds3=%PYARROW_WITH_S3%" || exit /B 1
%PYTHON_CMD% -m pip install --no-index --find-links .\dist\ pyarrow

popd
