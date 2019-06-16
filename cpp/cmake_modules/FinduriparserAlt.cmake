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

if(uriparser_ROOT)
  find_library(uriparser_LIB
               NAMES uriparser
               PATHS ${uriparser_ROOT}
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
               NO_DEFAULT_PATH)
  find_path(uriparser_INCLUDE_DIR
            NAMES uriparser/Uri.h
            PATHS ${uriparser_ROOT}
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES}
            NO_DEFAULT_PATH)
else()
  set(uriparser_PC_MODULE liburiparser)
  if(uriparserAlt_FIND_VERSION)
    set(uriparser_PC_MODULE "${uriparser_PC_MODULE} >= ${uriparserAlt_FIND_VERSION}")
  endif()
  pkg_check_modules(uriparser_PC ${uriparser_PC_MODULE})
  if(uriparser_PC_FOUND)
    set(uriparser_VERSION "${uriparser_PC_VERSION}")
    set(uriparser_INCLUDE_DIR "${uriparser_PC_INCLUDEDIR}")
    list(APPEND uriparser_PC_LIBRARY_DIRS "${uriparser_PC_LIBDIR}")
    find_library(uriparser_LIB uriparser
                 PATHS ${uriparser_PC_LIBRARY_DIRS}
                 PATH_SUFFIXES "${CMAKE_LIBRARY_ARCHITECTURE}"
                 NO_DEFAULT_PATH)
  else()
    find_library(uriparser_LIB NAMES uriparser PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
    find_path(uriparser_INCLUDE_DIR
              NAMES uriparser/Uri.h
              PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
  endif()
endif()

if(NOT uriparser_VERSION AND uriparser_INCLUDE_DIR)
  file(READ "${uriparser_INCLUDE_DIR}/uriparser/UriBase.h" uriparser_URI_BASE_H_CONTENT)
  string(REGEX MATCH "#define URI_VER_MAJOR +[0-9]+" uriparser_MAJOR_VERSION_DEFINITION
               "${uriparser_URI_BASE_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ +([0-9]+)$" "\\1" uriparser_MAJOR_VERSION
                 "${uriparser_MAJOR_VERSION_DEFINITION}")
  string(REGEX MATCH "#define URI_VER_MINOR +[0-9]+" uriparser_MINOR_VERSION_DEFINITION
               "${uriparser_URI_BASE_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ +([0-9]+)$" "\\1" uriparser_MINOR_VERSION
                 "${uriparser_MINOR_VERSION_DEFINITION}")
  string(REGEX MATCH "#define URI_VER_RELEASE +[0-9]+"
               uriparser_RELEASE_VERSION_DEFINITION "${uriparser_URI_BASE_H_CONTENT}")
  string(REGEX
         REPLACE "^.+ +([0-9]+)$" "\\1" uriparser_RELEASE_VERSION
                 "${uriparser_RELEASE_VERSION_DEFINITION}")
  if("${uriparser_MAJOR_VERSION}" STREQUAL ""
     OR "${uriparser_MINOR_VERSION}" STREQUAL ""
     OR "${uriparser_RELEASE_VERSION}" STREQUAL "")
    set(uriparser_VERSION "0.0.0")
  else()
    set(
      uriparser_VERSION

      "${uriparser_MAJOR_VERSION}.${uriparser_MINOR_VERSION}.${uriparser_RELEASE_VERSION}"
      )
  endif()
endif()

find_package_handle_standard_args(uriparserAlt
                                  REQUIRED_VARS
                                  uriparser_LIB
                                  uriparser_INCLUDE_DIR
                                  VERSION_VAR
                                  uriparser_VERSION)

if(uriparserAlt_FOUND)
  add_library(uriparser::uriparser UNKNOWN IMPORTED)
  set_target_properties(uriparser::uriparser
                        PROPERTIES IMPORTED_LOCATION "${uriparser_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${uriparser_INCLUDE_DIR}"
                                   # URI_STATIC_BUILD required on Windows
                                   INTERFACE_COMPILE_DEFINITIONS "URI_NO_UNICODE")
endif()
