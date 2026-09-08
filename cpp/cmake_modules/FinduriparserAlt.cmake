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

# Find uriparser.
#
# Debian, Ubuntu and several other distributions ship a liburiparser-dev that
# provides only a pkg-config file, while upstream (and vcpkg, conda-forge)
# also install a CMake package configuration. Try the CMake config first, then
# fall back to pkg-config and finally to a plain library search.

if(uriparserAlt_FOUND)
  return()
endif()

set(find_package_args)
if(uriparserAlt_FIND_VERSION)
  list(APPEND find_package_args ${uriparserAlt_FIND_VERSION})
endif()
if(uriparserAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(uriparser ${find_package_args} CONFIG)
if(uriparser_FOUND)
  set(uriparserAlt_FOUND TRUE)
  return()
endif()

if(URIPARSER_ROOT)
  find_library(URIPARSER_LIB
               NAMES uriparser
               PATHS ${URIPARSER_ROOT}
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(URIPARSER_INCLUDE_DIR
            NAMES uriparser/Uri.h
            PATHS ${URIPARSER_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_package(PkgConfig QUIET)
  pkg_check_modules(URIPARSER_PC liburiparser)
  if(URIPARSER_PC_FOUND)
    set(URIPARSER_INCLUDE_DIR "${URIPARSER_PC_INCLUDEDIR}")
    list(APPEND URIPARSER_PC_LIBRARY_DIRS "${URIPARSER_PC_LIBDIR}")
    find_library(URIPARSER_LIB
                 NAMES uriparser
                 PATHS ${URIPARSER_PC_LIBRARY_DIRS}
                 NO_DEFAULT_PATH
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    set(URIPARSER_VERSION "${URIPARSER_PC_VERSION}")
  else()
    find_library(URIPARSER_LIB
                 NAMES uriparser
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(URIPARSER_INCLUDE_DIR
              NAMES uriparser/Uri.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
endif()

find_package_handle_standard_args(uriparserAlt
                                  REQUIRED_VARS
                                  URIPARSER_LIB
                                  URIPARSER_INCLUDE_DIR
                                  VERSION_VAR
                                  URIPARSER_VERSION)

if(uriparserAlt_FOUND)
  if(NOT TARGET uriparser::uriparser)
    add_library(uriparser::uriparser UNKNOWN IMPORTED)
    set_target_properties(uriparser::uriparser
                          PROPERTIES IMPORTED_LOCATION "${URIPARSER_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${URIPARSER_INCLUDE_DIR}")
  endif()
endif()
