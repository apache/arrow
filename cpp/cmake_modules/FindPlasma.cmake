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

# - Find Plasma (plasma/client.h, libplasma.a, libplasma.so)
#
# This module requires Arrow from which it uses
#  arrow_find_package()
#
# This module defines
#  PLASMA_EXECUTABLE, deprecated. Use PLASMA_STORE_SERVER instead
#  PLASMA_FOUND, whether Plasma has been found
#  PLASMA_IMPORT_LIB, path to libplasma's import library (Windows only)
#  PLASMA_INCLUDE_DIR, directory containing headers
#  PLASMA_LIBS, deprecated. Use PLASMA_LIB_DIR instead
#  PLASMA_LIB_DIR, directory containing Plasma libraries
#  PLASMA_SHARED_IMP_LIB, deprecated. Use PLASMA_IMPORT_LIB instead
#  PLASMA_SHARED_LIB, path to libplasma's shared library
#  PLASMA_SO_VERSION, shared object version of found Plasma such as "100"
#  PLASMA_STATIC_LIB, path to libplasma.a
#  PLASMA_STORE_SERVER, path to plasma-store-server

if(DEFINED PLASMA_FOUND)
  return()
endif()

set(find_package_arguments)
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION)
  list(APPEND find_package_arguments "${${CMAKE_FIND_PACKAGE_NAME}_FIND_VERSION}")
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_REQUIRED)
  list(APPEND find_package_arguments REQUIRED)
endif()
if(${CMAKE_FIND_PACKAGE_NAME}_FIND_QUIETLY)
  list(APPEND find_package_arguments QUIET)
endif()
find_package(Arrow ${find_package_arguments})

if(ARROW_FOUND)
  arrow_find_package(PLASMA
                     "${ARROW_HOME}"
                     plasma
                     plasma/client.h
                     Plasma
                     plasma)
  if(ARROW_HOME)
    set(PLASMA_STORE_SERVER
        ${ARROW_HOME}/bin/plasma-store-server${CMAKE_EXECUTABLE_SUFFIX})
  else()
    if(PLASMA_USE_CMAKE_PACKAGE_CONFIG)
      find_package(Plasma CONFIG)
    elseif(PLASMA_USE_PKG_CONFIG)
      pkg_get_variable(PLASMA_STORE_SERVER plasma plasma_store_server)
    endif()
  endif()
  set(PLASMA_VERSION "${ARROW_VERSION}")
  set(PLASMA_SO_VERSION "${ARROW_SO_VERSION}")
  set(PLASMA_ABI_VERSION "${PLASMA_SO_VERSION}")
  # For backward compatibility
  set(PLASMA_EXECUTABLE "${PLASMA_STORE_SERVER}")
  set(PLASMA_LIBS "${PLASMA_LIB_DIR}")
endif()

mark_as_advanced(PLASMA_ABI_VERSION
                 PLASMA_EXECUTABLE
                 PLASMA_IMPORT_LIB
                 PLASMA_INCLUDE_DIR
                 PLASMA_LIBS
                 PLASMA_LIB_DIR
                 PLASMA_SHARED_IMP_LIB
                 PLASMA_SHARED_LIB
                 PLASMA_SO_VERSION
                 PLASMA_STATIC_LIB
                 PLASMA_STORE_SERVER
                 PLASMA_VERSION)

find_package_handle_standard_args(Plasma
                                  REQUIRED_VARS
                                  PLASMA_INCLUDE_DIR
                                  PLASMA_LIB_DIR
                                  PLASMA_SO_VERSION
                                  PLASMA_STORE_SERVER
                                  VERSION_VAR
                                  PLASMA_VERSION)
set(PLASMA_FOUND ${Plasma_FOUND})

if(Plasma_FOUND AND NOT Plasma_FIND_QUIETLY)
  message(STATUS "Found the Plasma by ${PLASMA_FIND_APPROACH}")
  message(STATUS "Found the plasma-store-server: ${PLASMA_STORE_SERVER}")
  message(STATUS "Found the Plasma shared library: ${PLASMA_SHARED_LIB}")
  message(STATUS "Found the Plasma import library: ${PLASMA_IMPORT_LIB}")
  message(STATUS "Found the Plasma static library: ${PLASMA_STATIC_LIB}")
endif()
