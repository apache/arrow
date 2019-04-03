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

if "%MSYSTEM%" == "MINGW32" (
  set "PATH=c:\Ruby26\bin;%PATH%"
) else (
  set "PATH=c:\Ruby26-x64\bin;%PATH%"
)
set PATH=%MINGW_PREFIX%\bin;C:\msys64\usr\bin;%PATH%

pacman --sync --refresh --noconfirm ^
    "%MINGW_PACKAGE_PREFIX%-boost" ^
    "%MINGW_PACKAGE_PREFIX%-brotli" ^
    "%MINGW_PACKAGE_PREFIX%-cmake" ^
    "%MINGW_PACKAGE_PREFIX%-double-conversion" ^
    "%MINGW_PACKAGE_PREFIX%-flatbuffers" ^
    "%MINGW_PACKAGE_PREFIX%-gflags" ^
    "%MINGW_PACKAGE_PREFIX%-gobject-introspection" ^
    "%MINGW_PACKAGE_PREFIX%-gtk-doc" ^
    "%MINGW_PACKAGE_PREFIX%-lz4" ^
    "%MINGW_PACKAGE_PREFIX%-meson" ^
    "%MINGW_PACKAGE_PREFIX%-protobuf" ^
    "%MINGW_PACKAGE_PREFIX%-python3-numpy" ^
    "%MINGW_PACKAGE_PREFIX%-rapidjson" ^
    "%MINGW_PACKAGE_PREFIX%-snappy" ^
    "%MINGW_PACKAGE_PREFIX%-thrift" ^
    "%MINGW_PACKAGE_PREFIX%-zlib" ^
    "%MINGW_PACKAGE_PREFIX%-zstd" || exit /B

pushd c_glib
ruby -S bundle install || exit /B
popd
