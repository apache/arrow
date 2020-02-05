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

if(MSVC AND NOT DEFINED LZ4_MSVC_STATIC_LIB_SUFFIX)
  set(LZ4_MSVC_STATIC_LIB_SUFFIX "_static")
endif()

set(LZ4_STATIC_LIB_SUFFIX "${LZ4_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}")

set(LZ4_STATIC_LIB_NAME ${CMAKE_STATIC_LIBRARY_PREFIX}lz4${LZ4_STATIC_LIB_SUFFIX})

if(LZ4_ROOT)
  find_library(
    LZ4_LIB
    NAMES lz4 ${LZ4_STATIC_LIB_NAME} lib${LZ4_STATIC_LIB_NAME}
          "${CMAKE_SHARED_LIBRARY_PREFIX}lz4_static${CMAKE_SHARED_LIBRARY_SUFFIX}"
    PATHS ${LZ4_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_path(LZ4_INCLUDE_DIR
            NAMES lz4.h
            PATHS ${LZ4_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})

else()
  pkg_check_modules(LZ4_PC liblz4)
  if(LZ4_PC_FOUND)
    set(LZ4_INCLUDE_DIR "${LZ4_PC_INCLUDEDIR}")

    list(APPEND LZ4_PC_LIBRARY_DIRS "${LZ4_PC_LIBDIR}")
    find_library(LZ4_LIB lz4
                 PATHS ${LZ4_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  else()
    find_library(
      LZ4_LIB
      NAMES lz4 ${LZ4_STATIC_LIB_NAME} lib${LZ4_STATIC_LIB_NAME}
            "${CMAKE_SHARED_LIBRARY_PREFIX}lz4_static${CMAKE_SHARED_LIBRARY_SUFFIX}"
      PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
    find_path(LZ4_INCLUDE_DIR NAMES lz4.h PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(Lz4 REQUIRED_VARS LZ4_LIB LZ4_INCLUDE_DIR)

# CMake 3.2 does uppercase the FOUND variable
if(Lz4_FOUND OR LZ4_FOUND)
  set(Lz4_FOUND TRUE)
  add_library(LZ4::lz4 UNKNOWN IMPORTED)
  set_target_properties(LZ4::lz4
                        PROPERTIES IMPORTED_LOCATION "${LZ4_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${LZ4_INCLUDE_DIR}")
endif()
