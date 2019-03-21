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

set CMAKE_BUILD_TYPE=release
set MESON_BUILD_TYPE=release

set INSTALL_DIR=%HOMEDRIVE%%HOMEPATH%\install
set PATH=%INSTALL_DIR%\bin;%PATH%
set PKG_CONFIG_PATH=%INSTALL_DIR%\lib\pkgconfig
set GI_TYPELIB_PATH=%INSTALL_DIR%\lib\girepository-1.0
set ARROW_DLL_PATH=%MINGW_PREFIX%\bin
set ARROW_DLL_PATH=%INSTALL_DIR%\bin;%ARROW_DLL_PATH%

for /f "usebackq" %%v in (`python3 -c "import sys; print('.'.join(map(str, sys.version_info[0:2])))"`) do (
  set PYTHON_VERSION=%%v
)

set CPP_BUILD_DIR=cpp\build
mkdir %CPP_BUILD_DIR%
pushd %CPP_BUILD_DIR%

cmake ^
    -G "MSYS Makefiles" ^
    -DCMAKE_INSTALL_PREFIX=%INSTALL_DIR% ^
    -DCMAKE_BUILD_TYPE=%CMAKE_BUILD_TYPE% ^
    -DARROW_VERBOSE_THIRDPARTY_BUILD=OFF ^
    -DARROW_PACKAGE_PREFIX=%MINGW_PREFIX% ^
    -DARROW_JEMALLOC=OFF ^
    -DARROW_USE_GLOG=OFF ^
    -DARROW_PYTHON=ON ^
    -DPythonInterp_FIND_VERSION=ON ^
    -DPythonInterp_FIND_VERSION_MAJOR=3 ^
    -DARROW_BUILD_TESTS=ON ^
    .. || exit /B
make -j4 || exit /B
setlocal
set PYTHONHOME=%MINGW_PREFIX%\lib\python%PYTHON_VERSION%
set PYTHONPATH=%PYTHONHOME%
set PYTHONPATH=%PYTHONPATH%;%MINGW_PREFIX%\lib\python%PYTHON_VERSION%\lib-dynload
set PYTHONPATH=%PYTHONPATH%;%MINGW_PREFIX%\lib\python%PYTHON_VERSION%\site-packages
@rem TODO(ARROW-4784): Run all tests
ctest ^
  --exclude-regex "arrow-array-test|arrow-thread-pool-test" ^
  --output-on-failure ^
  --parallel 2 || exit /B
endlocal
make install || exit /B
popd

set C_GLIB_BUILD_DIR=c_glib\build
meson ^
    setup ^
    --prefix=%INSTALL_DIR% ^
    --buildtype=%MESON_BUILD_TYPE% ^
    %C_GLIB_BUILD_DIR% ^
    c_glib || exit /B
sed -i'' -s 's/\r//g' %C_GLIB_BUILD_DIR%/arrow-glib/version.h || exit /B
ninja -C %C_GLIB_BUILD_DIR% || exit /B
ninja -C %C_GLIB_BUILD_DIR% install || exit /B
ruby c_glib\test\run-test.rb || exit /B
