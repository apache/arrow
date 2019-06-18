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

pkg_check_modules(RE2_PC re2)
if(RE2_PC_FOUND)
  set(RE2_INCLUDE_DIR "${RE2_PC_INCLUDEDIR}")

  list(APPEND RE2_PC_LIBRARY_DIRS "${RE2_PC_LIBDIR}")
  find_library(RE2_LIB re2
               PATHS ${RE2_PC_LIBRARY_DIRS}
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)

  # On Fedora, the reported prefix is wrong. As users likely run into this,
  # workaround.
  # https://bugzilla.redhat.com/show_bug.cgi?id=1652589
  if(UNIX AND NOT APPLE AND NOT RE2_LIB)
    if(RE2_PC_PREFIX STREQUAL "/usr/local")
      find_library(RE2_LIB re2)
    endif()
  endif()
elseif(RE2_ROOT)
  find_library(
    RE2_LIB
    NAMES
      re2
      "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}"
    PATHS ${RE2_ROOT}
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
    NO_DEFAULT_PATH)
  find_path(RE2_INCLUDE_DIR
            NAMES re2/re2.h
            PATHS ${RE2_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
else()
  find_library(
    RE2_LIB
    NAMES
      re2
      "${CMAKE_STATIC_LIBRARY_PREFIX}re2${RE2_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}"
      "${CMAKE_SHARED_LIBRARY_PREFIX}re2${CMAKE_SHARED_LIBRARY_SUFFIX}"
    PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(RE2_INCLUDE_DIR NAMES re2/re2.h PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
endif()

find_package_handle_standard_args(RE2 REQUIRED_VARS RE2_LIB RE2_INCLUDE_DIR)

if(RE2_FOUND)
  add_library(RE2::re2 UNKNOWN IMPORTED)
  set_target_properties(RE2::re2
                        PROPERTIES IMPORTED_LOCATION "${RE2_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES "${RE2_INCLUDE_DIR}")
endif()
