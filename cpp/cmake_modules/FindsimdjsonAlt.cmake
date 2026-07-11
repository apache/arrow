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

if(SimdjsonAlt_FOUND)
  return()
endif()

# Try to find simdjson manually first, as some system packages have broken
# CMake configs that reference non-existent library files (e.g., Alpine Linux)
if(simdjson_ROOT)
  find_path(SIMDJSON_INCLUDE_DIR
            NAMES simdjson.h
            PATHS ${simdjson_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES "include")
  find_library(SIMDJSON_LIBRARY
               NAMES simdjson
               PATHS ${simdjson_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES "lib" "lib64")
else()
  find_path(SIMDJSON_INCLUDE_DIR
            NAMES simdjson.h
            PATH_SUFFIXES "include")
  find_library(SIMDJSON_LIBRARY
               NAMES simdjson
               PATH_SUFFIXES "lib" "lib64")
endif()

if(SIMDJSON_INCLUDE_DIR AND SIMDJSON_LIBRARY)
  # Found via manual search
  file(READ "${SIMDJSON_INCLUDE_DIR}/simdjson.h" SIMDJSON_H_CONTENT)
  string(REGEX MATCH "#define SIMDJSON_VERSION \"([0-9]+\\.[0-9]+\\.[0-9]+)\""
               SIMDJSON_VERSION_DEFINITION "${SIMDJSON_H_CONTENT}")
  string(REGEX REPLACE "^.+ \"([0-9]+\\.[0-9]+\\.[0-9]+)\"$" "\\1" SIMDJSON_VERSION
                       "${SIMDJSON_VERSION_DEFINITION}")
  if("${SIMDJSON_VERSION}" STREQUAL "")
    set(SIMDJSON_VERSION "0.0.0")
  endif()

  find_package_handle_standard_args(
    SimdjsonAlt
    REQUIRED_VARS SIMDJSON_INCLUDE_DIR SIMDJSON_LIBRARY
    VERSION_VAR SIMDJSON_VERSION)

  if(SimdjsonAlt_FOUND)
    if(WIN32 AND "${SIMDJSON_INCLUDE_DIR}" MATCHES "^/")
      # MSYS2
      execute_process(COMMAND "cygpath" "--windows" "${SIMDJSON_INCLUDE_DIR}"
                      OUTPUT_VARIABLE SIMDJSON_INCLUDE_DIR
                      OUTPUT_STRIP_TRAILING_WHITESPACE)
    endif()
    # Detect library type based on file extension
    if("${SIMDJSON_LIBRARY}" MATCHES "\\.(so|dylib)(\\.[0-9]+)*$" OR "${SIMDJSON_LIBRARY}"
                                                                     MATCHES "\\.dll$")
      add_library(simdjson::simdjson SHARED IMPORTED)
    else()
      add_library(simdjson::simdjson STATIC IMPORTED)
    endif()
    set_target_properties(simdjson::simdjson
                          PROPERTIES IMPORTED_LOCATION "${SIMDJSON_LIBRARY}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${SIMDJSON_INCLUDE_DIR}")
  endif()
  return()
endif()

# Manual search failed, try CMake config mode
set(find_package_args)
if(SimdjsonAlt_FIND_VERSION)
  list(APPEND find_package_args ${SimdjsonAlt_FIND_VERSION})
endif()
if(SimdjsonAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(simdjson ${find_package_args} CONFIG)
if(simdjson_FOUND)
  set(SimdjsonAlt_FOUND TRUE)
  if(NOT TARGET simdjson::simdjson)
    # simdjson's CMake config should create this target, but create it if missing
    if(TARGET simdjson)
      add_library(simdjson::simdjson ALIAS simdjson)
    endif()
  endif()
endif()
