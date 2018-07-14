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
# Tries to find Flatbuffers headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(Flatbuffers)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  Flatbuffers_HOME -
#   When set, this path is inspected instead of standard library locations as
#   the root of the Flatbuffers installation.  The environment variable
#   FLATBUFFERS_HOME overrides this veriable.
#
# This module defines
#  FLATBUFFERS_INCLUDE_DIR, directory containing headers
#  FLATBUFFERS_LIBS, directory containing flatbuffers libraries
#  FLATBUFFERS_STATIC_LIB, path to libflatbuffers.a
#  FLATBUFFERS_FOUND, whether flatbuffers has been found

if( NOT "${FLATBUFFERS_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${FLATBUFFERS_HOME}" _native_path )
    list( APPEND _flatbuffers_roots "${_native_path}" )
elseif ( Flatbuffers_HOME )
    list( APPEND _flatbuffers_roots "${Flatbuffers_HOME}" )
endif()

# Try the parameterized roots, if they exist
if ( _flatbuffers_roots )
    find_path( FLATBUFFERS_INCLUDE_DIR NAMES flatbuffers/flatbuffers.h
        PATHS "${_flatbuffers_roots}" NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( FLATBUFFERS_LIBRARIES NAMES flatbuffers
        PATHS "${_flatbuffers_roots}" NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" "lib64")
else ()
    find_path( FLATBUFFERS_INCLUDE_DIR NAMES flatbuffers/flatbuffers.h )
    find_library( FLATBUFFERS_LIBRARIES NAMES flatbuffers )
endif ()

find_program(FLATBUFFERS_COMPILER flatc
  "${FLATBUFFERS_HOME}/bin"
  /usr/local/bin
  /usr/bin
  NO_DEFAULT_PATH
)

if (FLATBUFFERS_INCLUDE_DIR AND FLATBUFFERS_LIBRARIES)
  set(FLATBUFFERS_FOUND TRUE)
  get_filename_component( FLATBUFFERS_LIBS "${FLATBUFFERS_LIBRARIES}" PATH )
  set(FLATBUFFERS_LIB_NAME libflatbuffers)
  set(FLATBUFFERS_STATIC_LIB "${FLATBUFFERS_LIBS}/${FLATBUFFERS_LIB_NAME}.a")
else ()
  set(FLATBUFFERS_FOUND FALSE)
endif ()

if (FLATBUFFERS_FOUND)
  if (NOT Flatbuffers_FIND_QUIETLY)
    message(STATUS "Found the Flatbuffers library: ${FLATBUFFERS_LIBRARIES}")
  endif ()
else ()
  if (NOT Flatbuffers_FIND_QUIETLY)
    set(FLATBUFFERS_ERR_MSG "Could not find the Flatbuffers library. Looked in ")
    if ( _flatbuffers_roots )
      set(FLATBUFFERS_ERR_MSG "${FLATBUFFERS_ERR_MSG} ${_flatbuffers_roots}.")
    else ()
      set(FLATBUFFERS_ERR_MSG "${FLATBUFFERS_ERR_MSG} system search paths.")
    endif ()
    if (Flatbuffers_FIND_REQUIRED)
      message(FATAL_ERROR "${FLATBUFFERS_ERR_MSG}")
    else (Flatbuffers_FIND_REQUIRED)
      message(STATUS "${FLATBUFFERS_ERR_MSG}")
    endif (Flatbuffers_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  FLATBUFFERS_INCLUDE_DIR
  FLATBUFFERS_LIBS
  FLATBUFFERS_STATIC_LIB
  FLATBUFFERS_COMPILER
)
