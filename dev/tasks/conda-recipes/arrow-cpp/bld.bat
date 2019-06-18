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

mkdir "%SRC_DIR%"\cpp\build
pushd "%SRC_DIR%"\cpp\build

cmake -G "%CMAKE_GENERATOR%" ^
      -DCMAKE_INSTALL_PREFIX="%LIBRARY_PREFIX%" ^
      -DARROW_DEPENDENCY_SOURCE=SYSTEM ^
      -DARROW_PACKAGE_PREFIX="%LIBRARY_PREFIX%" ^
      -DLLVM_TOOLS_BINARY_DIR="%LIBRARY_BIN%" ^
      -DARROW_BOOST_USE_SHARED:BOOL=ON ^
      -DARROW_BUILD_TESTS:BOOL=OFF ^
      -DARROW_BUILD_UTILITIES:BOOL=OFF ^
      -DCMAKE_BUILD_TYPE=release ^
      -DARROW_PYTHON:BOOL=ON ^
      -DARROW_PARQUET:BOOL=ON ^
      -DARROW_GANDIVA:BOOL=ON ^
      -DARROW_ORC:BOOL=ON ^
      ..

cmake --build . --target INSTALL --config Release

popd
