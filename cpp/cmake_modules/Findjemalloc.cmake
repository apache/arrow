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
# Tries to find jemalloc headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(jemalloc)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  JEMALLOC_HOME -
#   When set, this path is inspected instead of standard library locations as
#   the root of the jemalloc installation.  The environment variable
#   JEMALLOC_HOME overrides this veriable.
#
# This module defines
#  JEMALLOC_INCLUDE_DIR, directory containing headers
#  JEMALLOC_SHARED_LIB, path to libjemalloc.so/dylib
#  JEMALLOC_FOUND, whether flatbuffers has been found

if( NOT "$ENV{JEMALLOC_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{JEMALLOC_HOME}" _native_path )
    list( APPEND _jemalloc_roots ${_native_path} )
elseif ( JEMALLOC_HOME )
    list( APPEND _jemalloc_roots ${JEMALLOC_HOME} )
endif()

set(LIBJEMALLOC_NAMES jemalloc libjemalloc.so.1 libjemalloc.so.2 libjemalloc.dylib)

# Try the parameterized roots, if they exist
if ( _jemalloc_roots )
    find_path( JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h
        PATHS ${_jemalloc_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( JEMALLOC_SHARED_LIB NAMES ${LIBJEMALLOC_NAMES}
        PATHS ${_jemalloc_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
    find_library( JEMALLOC_STATIC_LIB NAMES jemalloc_pic
        PATHS ${_jemalloc_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
else ()
    find_path( JEMALLOC_INCLUDE_DIR NAMES jemalloc/jemalloc.h )
    message(STATUS ${JEMALLOC_INCLUDE_DIR})
    find_library( JEMALLOC_SHARED_LIB NAMES ${LIBJEMALLOC_NAMES})
    message(STATUS ${JEMALLOC_SHARED_LIB})
    find_library( JEMALLOC_STATIC_LIB NAMES jemalloc_pic)
    message(STATUS ${JEMALLOC_STATIC_LIB})
endif ()

if (JEMALLOC_INCLUDE_DIR AND JEMALLOC_SHARED_LIB)
  set(JEMALLOC_FOUND TRUE)
else ()
  set(JEMALLOC_FOUND FALSE)
endif ()

if (JEMALLOC_FOUND)
    if (NOT jemalloc_FIND_QUIETLY)
      message(STATUS "Found the jemalloc library: ${JEMALLOC_LIBRARIES}")
  endif ()
else ()
  if (NOT jemalloc_FIND_QUIETLY)
    set(JEMALLOC_ERR_MSG "Could not find the jemalloc library. Looked in ")
    if ( _flatbuffers_roots )
      set(JEMALLOC_ERR_MSG "${JEMALLOC_ERR_MSG} in ${_jemalloc_roots}.")
    else ()
      set(JEMALLOC_ERR_MSG "${JEMALLOC_ERR_MSG} system search paths.")
    endif ()
    if (jemalloc_FIND_REQUIRED)
      message(FATAL_ERROR "${JEMALLOC_ERR_MSG}")
    else (jemalloc_FIND_REQUIRED)
      message(STATUS "${JEMALLOC_ERR_MSG}")
    endif (jemalloc_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  JEMALLOC_INCLUDE_DIR
  JEMALLOC_SHARED_LIB
)
