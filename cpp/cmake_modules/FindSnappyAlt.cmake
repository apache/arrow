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

set(find_package_args)
if(SnappyAlt_FIND_VERSION)
  list(APPEND find_package_args ${SnappyAlt_FIND_VERSION})
endif()
if(SnappyAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(Snappy ${find_package_args})
if(Snappy_FOUND)
  set(SnappyAlt_FOUND TRUE)
  return()
endif()

if(ARROW_SNAPPY_USE_SHARED)
  set(SnappyAlt_LIB_NAMES)
  if(CMAKE_IMPORT_LIBRARY_SUFFIX)
    list(APPEND SnappyAlt_LIB_NAMES
         "${CMAKE_IMPORT_LIBRARY_PREFIX}snappy${CMAKE_IMPORT_LIBRARY_SUFFIX}")
  endif()
  list(APPEND SnappyAlt_LIB_NAMES
       "${CMAKE_SHARED_LIBRARY_PREFIX}snappy${CMAKE_SHARED_LIBRARY_SUFFIX}")
else()
  set(SnappyAlt_STATIC_LIB_NAME_BASE "snappy")
  if(MSVC)
    set(SnappyAlt_STATIC_LIB_NAME_BASE
        "${SNAPPY_STATIC_LIB_NAME_BASE}${SNAPPY_MSVC_STATIC_LIB_SUFFIX}")
  endif()
  set(SnappyAlt_LIB_NAMES
      "${CMAKE_STATIC_LIBRARY_PREFIX}${SnappyAlt_STATIC_LIB_NAME_BASE}${CMAKE_STATIC_LIBRARY_SUFFIX}"
  )
endif()

find_package(PkgConfig QUIET)
pkg_check_modules(snappy_PC snappy)
if(snappy_PC_FOUND)
  set(SnappyAlt_VERSION "${snappy_PC_VERSION}")
  set(SnappyAlt_INCLUDE_DIR "${snappy_PC_INCLUDEDIR}")
  if(ARROW_SNAPPY_USE_SHARED)
    set(SnappyAlt_LINK_LIBRARIES ${snappy_PC_LINK_LIBRARIES})
    set(SnappyAlt_LINK_OPTIONS ${snappy_PC_LDFLAGS_OTHER})
    set(SnappyAlt_COMPILE_OPTIONS ${snappy_PC_CFLAGS_OTHER})
  else()
    set(SnappyAlt_LINK_LIBRARIES)
    foreach(SnappyAlt_LIBRARY_NAME ${snappy_PC_STATIC_LIBRARIES})
      find_library(SnappyAlt_LIBRARY_${SnappyAlt_LIBRARY_NAME}
                   NAMES "${CMAKE_STATIC_LIBRARY_PREFIX}${SnappyAlt_LIBRARY_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
                   HINTS ${snappy_PC_STATIC_LIBRARY_DIRS})
      list(APPEND SnappyAlt_LINK_LIBRARIES
           "${SnappyAlt_LIBRARY_${SnappyAlt_LIBRARY_NAME}}")
    endforeach()
    set(SnappyAlt_LINK_OPTIONS ${snappy_PC_STATIC_LDFLAGS_OTHER})
    set(SnappyAlt_COMPILE_OPTIONS ${snappy_PC_STATIC_CFLAGS_OTHER})
  endif()
  list(GET SnappyAlt_LINK_LIBRARIES 0 SnappyAlt_IMPORTED_LOCATION)
  list(REMOVE_AT SnappyAlt_LINK_LIBRARIES 0)
  find_package_handle_standard_args(
    SnappyAlt
    REQUIRED_VARS SnappyAlt_IMPORTED_LOCATION
    VERSION_VAR SnappyAlt_VERSION)
else()
  if(Snappy_ROOT)
    find_library(SnappyAlt_IMPORTED_LOCATION
                 NAMES ${SnappyAlt_LIB_NAMES}
                 PATHS ${Snappy_ROOT}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES}
                 NO_DEFAULT_PATH)
    find_path(SnappyAlt_INCLUDE_DIR
              NAMES snappy.h
              PATHS ${Snappy_ROOT}
              NO_DEFAULT_PATH
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  else()
    find_library(SnappyAlt_IMPORTED_LOCATION ${SnappyAlt_LIB_NAMES}
                 PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
    find_path(SnappyAlt_INCLUDE_DIR
              NAMES snappy.h
              PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
  endif()
  find_package_handle_standard_args(SnappyAlt REQUIRED_VARS SnappyAlt_IMPORTED_LOCATION
                                                            SnappyAlt_INCLUDE_DIR)
endif()

if(SnappyAlt_FOUND)
  if(NOT TARGET Snappy::snappy)
    add_library(Snappy::snappy UNKNOWN IMPORTED)
    set_target_properties(Snappy::snappy
                          PROPERTIES IMPORTED_LOCATION "${SnappyAlt_IMPORTED_LOCATION}"
                                     INTERFACE_COMPILE_OPTIONS
                                     "${SnappyAlt_COMPILE_OPTIONS}"
                                     INTERFACE_INCLUDE_DIRECTORIES
                                     "${SnappyAlt_INCLUDE_DIR}"
                                     INTERFACE_LINK_OPTIONS "${SnappyAlt_LINK_OPTIONS}")
  endif()
endif()
