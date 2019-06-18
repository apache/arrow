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

pushd "%SRC_DIR%"\python

@rem the symlinks for cmake modules don't work here
del cmake_modules\BuildUtils.cmake
del cmake_modules\SetupCxxFlags.cmake
del cmake_modules\CompilerInfo.cmake
del cmake_modules\FindNumPy.cmake
del cmake_modules\FindPythonLibsNew.cmake
copy /Y "%SRC_DIR%\cpp\cmake_modules\BuildUtils.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\SetupCxxFlags.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\CompilerInfo.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\FindNumPy.cmake" cmake_modules\
copy /Y "%SRC_DIR%\cpp\cmake_modules\FindPythonLibsNew.cmake" cmake_modules\

SET ARROW_HOME=%LIBRARY_PREFIX%
SET SETUPTOOLS_SCM_PRETEND_VERSION=%PKG_VERSION%
"%PYTHON%" setup.py ^
           build_ext --build-type=release ^
                     --with-parquet ^
                     --with-gandiva ^
           install --single-version-externally-managed ^
                   --record=record.txt
if errorlevel 1 exit 1
popd
