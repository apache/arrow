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

@rem clean up prior attempts
if exist "arrow-build" rd arrow-build /s /q
if exist "dist" rd dist /s /q
if exist "example" rd example /s /q

echo
echo "=="
echo "== Building Arrow C++ library"
echo "=="
echo

set INSTALL_PREFIX=%cd%\dist

mkdir arrow-build
pushd arrow-build

@rem bzip2_ep fails with this method

cmake ..\..\.. ^
    -GNinja ^
    -DCMAKE_INSTALL_PREFIX=%INSTALL_PREFIX% ^
    -DARROW_DEPENDENCY_SOURCE=BUNDLED ^
    -DARROW_BUILD_SHARED=OFF ^
    -DARROW_BUILD_STATIC=ON ^
    -DARROW_COMPUTE=ON ^
    -DARROW_CSV=ON ^
    -DARROW_DATASET=ON ^
    -DARROW_FILESYSTEM=ON ^
    -DARROW_HDFS=ON ^
    -DARROW_JSON=ON ^
    -DARROW_MIMALLOC=ON ^
    -DARROW_ORC=ON ^
    -DARROW_PARQUET=ON ^
    -DARROW_WITH_BROTLI=ON ^
    -DARROW_WITH_BZ2=OFF ^
    -DARROW_WITH_LZ4=ON ^
    -DARROW_WITH_SNAPPY=ON ^
    -DARROW_WITH_ZLIB=ON ^
    -DARROW_WITH_ZSTD=ON

ninja install

popd

echo
echo "=="
echo "== Building example project using Arrow C++ library"
echo "=="
echo

mkdir example
pushd example

cmake .. ^
      -GNinja ^
      -DCMAKE_PREFIX_PATH="%INSTALL_PREFIX%" ^
      -DARROW_LINK_SHARED=OFF
ninja

popd

echo
echo "=="
echo "== Running example project"
echo "=="
echo

call example\arrow-example.exe
