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

call "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Auxiliary\Build\vcvars64.bat"

echo "=== (%PYTHON_VERSION%) Clear output directories and leftovers ==="
del /s /q C:\arrow-build
del /s /q C:\arrow-dist
del /s /q C:\arrow\python\dist
del /s /q C:\arrow\python\build
del /s /q C:\arrow\python\pyarrow\*.so
del /s /q C:\arrow\python\pyarrow\*.so.*

echo "=== (%PYTHON_VERSION%) Building Arrow C++ libraries ==="
set ARROW_ACERO=ON
set ARROW_DATASET=ON
set ARROW_FLIGHT=ON
set ARROW_GANDIVA=OFF
set ARROW_GCS=ON
set ARROW_HDFS=ON
set ARROW_ORC=OFF
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
@rem Workaround for https://github.com/aws/aws-sdk-cpp/issues/1809 .
@rem Use (old) bundled AWS SDK C++ instead of (newer) AWS SDK C++.
set AWSSDK_SOURCE=BUNDLED
set CMAKE_UNITY_BUILD=ON
set CMAKE_GENERATOR=Visual Studio 15 2017 Win64
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
    -DAWSSDK_SOURCE=%AWSSDK_SOURCE% ^
    -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
    -DCMAKE_CXX_COMPILER=clcache ^
    -DCMAKE_INSTALL_PREFIX=C:\arrow-dist ^
    -DCMAKE_UNITY_BUILD=%CMAKE_UNITY_BUILD% ^
    -DMSVC_LINK_VERBOSE=ON ^
    -DPARQUET_REQUIRE_ENCRYPTION=%PARQUET_REQUIRE_ENCRYPTION% ^
    -DVCPKG_MANIFEST_MODE=OFF ^
    -DVCPKG_TARGET_TRIPLET=%VCGPK_TARGET_TRIPLET% ^
    -G "%CMAKE_GENERATOR%" ^
    C:\arrow\cpp || exit /B 1
cmake --build . --config %CMAKE_BUILD_TYPE% --target install || exit /B 1
popd

echo "=== (%PYTHON_VERSION%) Building wheel ==="
set PYARROW_BUILD_TYPE=%CMAKE_BUILD_TYPE%
set PYARROW_BUNDLE_ARROW_CPP=ON
set PYARROW_CMAKE_GENERATOR=%CMAKE_GENERATOR%
set PYARROW_INSTALL_TESTS=ON
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
@REM bundle the msvc runtime
cp "C:\Program Files (x86)\Microsoft Visual Studio\2017\Community\VC\Redist\MSVC\14.16.27012\x64\Microsoft.VC141.CRT\msvcp140.dll" pyarrow\
python setup.py bdist_wheel || exit /B 1
popd
