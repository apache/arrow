# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# Force some variables for Emscripten
# to disable things that won't work there

# # override default in Emscripten which is to not use shared libs
set_property(GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS TRUE)


# these are needed for building pyarrow
# if they aren't set, cmake cross compiling fails for python 
# modules (at least under pyodide it does)
set(Python3_INCLUDE_DIR $ENV{PYTHONINCLUDE})
set(Python3_LIBRARY $ENV{CPYTHONLIB})
set(Python3_NumPy_INCLUDE_DIR $ENV{NUMPY_LIB}/core/include)
set(Python3_EXECUTABLE)
set(ENV{_PYTHON_SYSCONFIGDATA_NAME} $ENV{SYSCONFIG_NAME})

# flags for creating shared libraries (only used in pyarrow, because
# emscripten builds libarrow as static)
set(CMAKE_C_FLAGS "-sUSE_ZLIB=1 -sSIDE_MODULE=1 -fPIC -fexceptions")
set(CMAKE_CXX_FLAGS "-sUSE_ZLIB=1 -sSIDE_MODULE=1 -fPIC -fexceptions")

set(CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")
set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")
set(CMAKE_SHARED_LINKER_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")

# stripping doesn't work on emscripten
set(CMAKE_STRIP FALSE)

