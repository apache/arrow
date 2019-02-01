#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Tries to find GTest headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(GTest)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  GTest_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the GTest/Gmock installation.
#                The environment variable GTEST_HOME overrides this variable.
#
# This module defines
#  GTEST_INCLUDE_DIR, directory containing headers
#  GTEST_LIBS, directory containing gtest libraries
#  GTEST_STATIC_LIB, path to libgtest.a
#  GTEST_STATIC_MAIN_LIB, path to libgtest_main.a
#  GTEST_SHARED_LIB, path to libgtest's shared library
#  GTEST_SHARED_MAIN_LIB, path to libgtest_main's shared library
#  GTEST_FOUND, whether gtest has been found
#
#  GMOCK_INCLUDE_DIR, directory containing headers
#  GMOCK_LIBS, directory containing gmock libraries
#  GMOCK_STATIC_LIB, path to libgmock.a
#  GMOCK_STATIC_MAIN_LIB, path to libgmock_main.a
#  GMOCK_SHARED_LIB, path to libgmock's shared library
#  GMOCK_SHARED_MAIN_LIB, path to libgmock_main's shared library
#  GMOCK_FOUND, whether gmock has been found

if( NOT "${GTEST_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${GTEST_HOME}" _native_path )
    list( APPEND _gtest_roots ${_native_path} )
elseif ( GTest_HOME )
    list( APPEND _gtest_roots ${GTest_HOME} )
endif()

set(GTEST_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}gtest${CMAKE_STATIC_LIBRARY_SUFFIX})
set(GTEST_MAIN_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}gtest_main${CMAKE_STATIC_LIBRARY_SUFFIX})
set(GTEST_SHARED_LIB_NAME
  ${CMAKE_SHARED_LIBRARY_PREFIX}gtest${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GTEST_MAIN_SHARED_LIB_NAME
  ${CMAKE_SHARED_LIBRARY_PREFIX}gtest_main${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GMOCK_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}gmock${CMAKE_STATIC_LIBRARY_SUFFIX})
set(GMOCK_MAIN_STATIC_LIB_NAME
  ${CMAKE_STATIC_LIBRARY_PREFIX}gmock_main${CMAKE_STATIC_LIBRARY_SUFFIX})
set(GMOCK_SHARED_LIB_NAME
  ${CMAKE_SHARED_LIBRARY_PREFIX}gmock${CMAKE_SHARED_LIBRARY_SUFFIX})
set(GMOCK_MAIN_SHARED_LIB_NAME
  ${CMAKE_SHARED_LIBRARY_PREFIX}gmock_main${CMAKE_SHARED_LIBRARY_SUFFIX})


# Try the parameterized roots, if they exist
if(_gtest_roots)
  find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
  set(lib_dirs
    "lib/${CMAKE_LIBRARY_ARCHITECTURE}"
    "lib64"
    "lib")
  find_library(GTEST_STATIC_LIB NAMES ${GTEST_STATIC_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GTEST_MAIN_STATIC_LIB NAMES ${GTEST_MAIN_STATIC_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GTEST_SHARED_LIB NAMES ${GTEST_SHARED_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GTEST_MAIN_SHARED_LIB NAMES ${GTEST_MAIN_SHARED_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
else()
  find_path(GTEST_INCLUDE_DIR NAMES gtest/gtest.h)
  find_library(GTEST_STATIC_LIB NAMES ${GTEST_STATIC_LIB_NAME})
  find_library(GTEST_MAIN_STATIC_LIB NAMES ${GTEST_MAIN_STATIC_LIB_NAME})
  find_library(GTEST_SHARED_LIB NAMES ${GTEST_SHARED_LIB_NAME})
  find_library(GTEST_MAIN_SHARED_LIB NAMES ${GTEST_MAIN_SHARED_LIB_NAME})
endif()

# gmock
# Try the parameterized roots, if they exist (reuse gtest because the
# libraries should be built together).
if(_gtest_roots)
  find_path(GMOCK_INCLUDE_DIR NAMES gmock/gmock.h
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
  set(lib_dirs
    "lib/${CMAKE_LIBRARY_ARCHITECTURE}"
    "lib64"
    "lib")
  find_library(GMOCK_STATIC_LIB NAMES ${GMOCK_STATIC_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GMOCK_MAIN_STATIC_LIB NAMES ${GMOCK_MAIN_STATIC_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GMOCK_SHARED_LIB NAMES ${GMOCK_SHARED_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
  find_library(GMOCK_MAIN_SHARED_LIB NAMES ${GMOCK_MAIN_SHARED_LIB_NAME}
    PATHS ${_gtest_roots} NO_DEFAULT_PATH
    PATH_SUFFIXES ${lib_dirs})
else()
  find_path(GMOCK_INCLUDE_DIR NAMES gmock/gmock.h)
  find_library(GMOCK_STATIC_LIB NAMES ${GMOCK_STATIC_LIB_NAME})
  find_library(GMOCK_MAIN_STATIC_LIB NAMES ${GMOCK_MAIN_STATIC_LIB_NAME})
  find_library(GMOCK_SHARED_LIB NAMES ${GMOCK_SHARED_LIB_NAME})
  find_library(GMOCK_MAIN_SHARED_LIB NAMES ${GMOCK_MAIN_SHARED_LIB_NAME})
endif()

if(GTEST_INCLUDE_DIR AND
    (GTEST_STATIC_LIB AND GTEST_MAIN_STATIC_LIB) OR
    (GTEST_SHARED_LIB AND GTEST_MAIN_SHARED_LIB))
  set(GTEST_FOUND TRUE)
else()
  set(GTEST_FOUND FALSE)
endif()

if(GMOCK_INCLUDE_DIR AND
    (GMOCK_STATIC_LIB AND GMOCK_MAIN_STATIC_LIB) OR
    (GMOCK_SHARED_LIB AND GMOCK_MAIN_SHARED_LIB))
  set(GMOCK_FOUND TRUE)
else()
  set(GMOCK_FOUND FALSE)
endif()

if (GTEST_FOUND)
  if (NOT GTest_FIND_QUIETLY)
    message(STATUS "Found the GTest library:")
    message(STATUS "GTEST_STATIC_LIB: ${GTEST_STATIC_LIB}")
    message(STATUS "GTEST_MAIN_STATIC_LIB: ${GTEST_MAIN_STATIC_LIB}")
    message(STATUS "GTEST_SHARED_LIB: ${GTEST_SHARED_LIB}")
    message(STATUS "GTEST_MAIN_SHARED_LIB: ${GTEST_MAIN_SHARED_LIB}")
  endif ()
else ()
  if (NOT GTest_FIND_QUIETLY)
    set(GTEST_ERR_MSG "Could not find the GTest library. Looked in ")
    if ( _gtest_roots )
      set(GTEST_ERR_MSG "${GTEST_ERR_MSG} in ${_gtest_roots}.")
    else ()
      set(GTEST_ERR_MSG "${GTEST_ERR_MSG} system search paths.")
    endif ()
    if (GTest_FIND_REQUIRED)
      message(FATAL_ERROR "${GTEST_ERR_MSG}")
    else (GTest_FIND_REQUIRED)
      message(STATUS "${GTEST_ERR_MSG}")
    endif (GTest_FIND_REQUIRED)
  endif ()
endif ()

if (GMOCK_FOUND)
  if (NOT GTest_FIND_QUIETLY)
    message(STATUS "Found the Gmock library:")
    message(STATUS "GMOCK_STATIC_LIB: ${GMOCK_STATIC_LIB}")
    message(STATUS "GMOCK_MAIN_STATIC_LIB: ${GMOCK_MAIN_STATIC_LIB}")
    message(STATUS "GMOCK_SHARED_LIB: ${GMOCK_SHARED_LIB}")
    message(STATUS "GMOCK_MAIN_SHARED_LIB: ${GMOCK_MAIN_SHARED_LIB}")
  endif ()
else ()
  # Reuse Gtest quietly and required flags because they should be found
  # in tandem.
  if (NOT GTest_FIND_QUIETLY)
    set(GMOCK_ERR_MSG "Could not find the GMock library. Looked in ")
    if ( _gtest_roots )
      set(GTEST_ERR_MSG "${GMOCK_ERR_MSG} in ${_gtest_roots}.")
    else ()
      set(GTEST_ERR_MSG "${GMOCK_ERR_MSG} system search paths.")
    endif ()
    if (GTest_FIND_REQUIRED)
      message(FATAL_ERROR "${GMOCK_ERR_MSG}")
    else (GTest_FIND_REQUIRED)
      message(STATUS "${GMOCK_ERR_MSG}")
    endif (GTest_FIND_REQUIRED)
  endif ()
endif ()


mark_as_advanced(
  GTEST_INCLUDE_DIR
  GTEST_LIBS
  GTEST_STATIC_LIB
  GTEST_MAIN_STATIC_LIB
  GTEST_SHARED_LIB
  GTEST_MAIN_SHARED_LIB

  GMOCK_INCLUDE_DIR
  GMOCK_LIBS
  GMOCK_STATIC_LIB
  GMOCK_MAIN_STATIC_LIB
  GMOCK_SHARED_LIB
  GMOCK_MAIN_SHARED_LIB
)
