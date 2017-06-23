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
# Tries to find Brotli headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Brotli)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Brotli_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the Brotli installation.
#                The environment variable BROTLI_HOME overrides this veriable.
#
# This module defines
#  BROTLI_INCLUDE_DIR, directory containing headers
#  BROTLI_LIBS, directory containing brotli libraries
#  BROTLI_STATIC_LIB, path to libbrotli.a
#  BROTLI_SHARED_LIB, path to libbrotli's shared library
#  BROTLI_FOUND, whether brotli has been found

if( NOT "${BROTLI_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${BROTLI_HOME}" _native_path )
    list( APPEND _brotli_roots ${_native_path} )
elseif ( Brotli_HOME )
    list( APPEND _brotli_roots ${Brotli_HOME} )
endif()

find_path( BROTLI_INCLUDE_DIR NAMES brotli/decode.h
  PATHS ${_brotli_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "include" )

find_library( BROTLI_LIBRARY_ENC NAMES libbrotlienc.a brotlienc
  PATHS ${_brotli_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib" )

find_library( BROTLI_LIBRARY_DEC NAMES libbrotlidec.a brotlidec
  PATHS ${_brotli_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib" )

find_library( BROTLI_LIBRARY_COMMON NAMES libbrotlicommon.a brotlicommon
  PATHS ${_brotli_roots}
  NO_DEFAULT_PATH
  PATH_SUFFIXES "lib/${CMAKE_LIBRARY_ARCHITECTURE}" "lib" )

set(BROTLI_LIBRARIES ${BROTLI_LIBRARY_ENC} ${BROTLI_LIBRARY_DEC}
    ${BROTLI_LIBRARY_COMMON})

if (BROTLI_INCLUDE_DIR AND (PARQUET_MINIMAL_DEPENDENCY OR BROTLI_LIBRARIES))
  set(BROTLI_FOUND TRUE)
  get_filename_component( BROTLI_LIBS ${BROTLI_LIBRARY_ENC} PATH )
  set(BROTLI_LIB_NAME brotli)
  if (MSVC AND NOT BROTLI_MSVC_STATIC_LIB_SUFFIX)
    set(BROTLI_MSVC_STATIC_LIB_SUFFIX _static)
  endif()
  set(BROTLI_STATIC_LIB
      ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}enc${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}dec${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX}
      ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}common${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(BROTLI_STATIC_LIBRARY_ENC ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}enc${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(BROTLI_STATIC_LIBRARY_DEC ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}dec${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(BROTLI_STATIC_LIBRARY_COMMON ${BROTLI_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${BROTLI_LIB_NAME}common${BROTLI_MSVC_STATIC_LIB_SUFFIX}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(BROTLI_SHARED_LIB
      ${BROTLI_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${BROTLI_LIB_NAME}enc${CMAKE_SHARED_LIBRARY_SUFFIX}
      ${BROTLI_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${BROTLI_LIB_NAME}dec${CMAKE_SHARED_LIBRARY_SUFFIX}
      ${BROTLI_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${BROTLI_LIB_NAME}common${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(BROTLI_FOUND FALSE)
endif ()

if (BROTLI_FOUND)
  if (NOT Brotli_FIND_QUIETLY)
    if (PARQUET_MINIMAL_DEPENDENCY)
      message(STATUS "Found the Brotli headers: ${BROTLI_INCLUDE_DIR}")
    else ()
      message(STATUS "Found the Brotli library: ${BROTLI_LIBRARIES}")
    endif ()
  endif ()
else ()
  if (NOT Brotli_FIND_QUIETLY)
    set(BROTLI_ERR_MSG "Could not find the Brotli library. Looked in ")
    if ( _brotli_roots )
      set(BROTLI_ERR_MSG "${BROTLI_ERR_MSG} in ${_brotli_roots}.")
    else ()
      set(BROTLI_ERR_MSG "${BROTLI_ERR_MSG} system search paths.")
    endif ()
    if (Brotli_FIND_REQUIRED)
      message(FATAL_ERROR "${BROTLI_ERR_MSG}")
    else (Brotli_FIND_REQUIRED)
      message(STATUS "${BROTLI_ERR_MSG}")
    endif (Brotli_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  BROTLI_INCLUDE_DIR
  BROTLI_LIBS
  BROTLI_LIBRARIES
  BROTLI_STATIC_LIB
  BROTLI_SHARED_LIB
)
