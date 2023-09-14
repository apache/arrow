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

# Force some variables for emscripten
# to disable things that won't work there

# make us be on the platforms list for cmake
get_filename_component(PLATFORM_FOLDER_PARENT ${CMAKE_CURRENT_LIST_DIR} DIRECTORY)
list(APPEND CMAKE_MODULE_PATH "${PLATFORM_FOLDER_PARENT}")

include($ENV{EMSDK}/upstream/emscripten/cmake/Modules/Platform/Emscripten.cmake)

# ensure zlib is built with -fpic
# and force us to link to the version in emscripten ports
if( NOT EXISTS ${EMSCRIPTEN_SYSROOT}/lib/wasm32-emscripten/pic/libz.a )
    execute_process( COMMAND embuilder --pic --force build zlib)
endif()
set(ZLIB_LIBRARY ${EMSCRIPTEN_SYSROOT}/lib/wasm32-emscripten/pic/libz.a)

# # override default in emscripten which is to not use shared libs
set_property(GLOBAL PROPERTY TARGET_SUPPORTS_SHARED_LIBS TRUE)

# if we leave the system name as Emscripten, then it reloads the original Emscripten.cmake every time a project() command
# is run, which does bad things like disabling shared libraries
set(CMAKE_SYSTEM_NAME EmscriptenOverrides)

set(CMAKE_C_FLAGS "-sUSE_ZLIB=1 -sSIDE_MODULE=1 -fPIC -fexceptions")
set(CMAKE_CXX_FLAGS "-sUSE_ZLIB=1 -sSIDE_MODULE=1 -fPIC -fexceptions")

#set(PYARROW_CPP_HOME "$ENV{ARROW_HOME}/lib")
#list(APPEND CMAKE_FIND_ROOT_PATH "${CMAKE_INSTALL_PREFIX}/cmake")

set(Python3_INCLUDE_DIR $ENV{PYTHONINCLUDE})
set(Python3_LIBRARY $ENV{CPYTHONLIB})
set(Python3_NumPy_INCLUDE_DIR $ENV{NUMPY_LIB}/core/include)
set(Python3_EXECUTABLE )
set(CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")
set(CMAKE_SHARED_LIBRARY_CREATE_CXX_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")
set(CMAKE_SHARED_LINKER_FLAGS "-sUSE_ZLIB=1 -sWASM_BIGINT=1 -fexceptions")
set(CMAKE_STRIP FALSE)  

set(ENV{_PYTHON_SYSCONFIGDATA_NAME} $ENV{SYSCONFIG_NAME})
