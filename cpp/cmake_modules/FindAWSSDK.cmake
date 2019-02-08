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
# Tries to find AWSSDK headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(AWSSDK COMPONENTS service1 service2 ...)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  AWSSDK_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the AWSSDK installation.
#                The environment variable AWSSDK_HOME overrides this veriable.
#
# This module defines
#  AWSSDK_INCLUDE_DIR, directory containing headers
#  AWSSDK_LIBRARY_DIR, directory containing libraries
#  AWSSDK_STATIC_LIBS, static AWS SDK libraries
#  AWSSDK_FOUND, whether AWS SDK has been found

if( NOT "${AWSSDK_HOME}" STREQUAL "")
  file( TO_CMAKE_PATH "${AWSSDK_HOME}" _native_path )
  list( APPEND _awssdk_roots ${_native_path} )
elseif ( AWSSDK_HOME )
  list( APPEND _awssdk_roots ${AWSSDK_HOME} )
endif()

set(AWSSDK_MODULES ${AWSSDK_FIND_COMPONENTS})
set(AWSSDK_STATIC_LIBS)

find_path(AWSSDK_INCLUDE_DIR NAMES aws/core/Aws.h
        PATHS ${_awssdk_roots}
        NO_DEFAULT_PATH
        PATH_SUFFIXES "include")

foreach(MODULE ${AWSSDK_MODULES})
  string(TOUPPER ${MODULE} MODULE_UPPERCASE)
  find_library( AWSSDK_STATIC_${MODULE_UPPERCASE}_LIB NAMES ${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-${MODULE}${CMAKE_STATIC_LIBRARY_SUFFIX}
          PATHS ${_awssdk_roots}
          NO_DEFAULT_PATH
          PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib" )
  # Fail if any module library is missing
  if (NOT AWSSDK_STATIC_${MODULE_UPPERCASE}_LIB)
    set(LIBRARY_NOT_FOUND "YES")
  else()
    list(APPEND AWSSDK_STATIC_LIBS ${AWSSDK_STATIC_${MODULE_UPPERCASE}_LIB})
  endif()
endforeach()

if (AWSSDK_INCLUDE_DIR AND NOT LIBRARY_NOT_FOUND)
  set(AWSSDK_FOUND TRUE)
  get_filename_component(AWSSDK_LIBRARY_DIR ${AWSSDK_STATIC_CORE_LIB} PATH)
else ()
  set(AWSSDK_FOUND FALSE)
endif ()

if (AWSSDK_FOUND)
  if (NOT AWSSDK_FIND_QUIETLY)
    message(STATUS "Found the AWSSDK libraries: ${AWSSDK_STATIC_LIBS}")
  endif ()
else ()
  set(AWSSDK_STATIC_LIBS)
  if (NOT AWSSDK_FIND_QUIETLY)
    set(AWSSDK_ERR_MSG "Could not find the AWSSDK library. Looked")
    if ( _awssdk_roots )
      set(AWSSDK_ERR_MSG "${AWSSDK_ERR_MSG} in ${_awssdk_roots}.")
    else ()
      set(AWSSDK_ERR_MSG "${AWSSDK_ERR_MSG} in system search paths.")
    endif ()
    if (AWSSDK_FIND_REQUIRED)
      message(FATAL_ERROR "${AWSSDK_ERR_MSG}")
    else (AWSSDK_FIND_REQUIRED)
      message(STATUS "${AWSSDK_ERR_MSG}")
    endif (AWSSDK_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
        AWSSDK_INCLUDE_DIR
        AWSSDK_LIBRARY_DIR
        AWSSDK_STATIC_LIBS
        AWSSDK_FOUND
)
