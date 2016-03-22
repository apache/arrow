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
# Tries to find Google benchmark headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(GBenchark)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  GBenchmark_HOME - When set, this path is inspected instead of standard library
#                    locations as the root of the benchark installation.
#                    The environment variable GBENCHMARK_HOME overrides this veriable.
#
# This module defines
#  GBENCHMARK_INCLUDE_DIR, directory containing benchmark header directory
#  GBENCHMARK_LIBS, directory containing benchmark libraries
#  GBENCHMARK_STATIC_LIB, path to libbenchmark.a
#  GBENCHMARK_FOUND, whether gbenchmark has been found

if( NOT "$ENV{GBENCHMARK_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "$ENV{GBENCHMARK_HOME}" _native_path )
    list( APPEND _gbenchmark_roots ${_native_path} )
elseif ( GBenchmark_HOME )
    list( APPEND _gbenchmark_roots ${GBenchmark_HOME} )
endif()

# Try the parameterized roots, if they exist
if ( _gbenchmark_roots )
    find_path( GBENCHMARK_INCLUDE_DIR NAMES benchmark/benchmark.h 
        PATHS ${_gbenchmark_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( GBENCHMARK_LIBRARIES NAMES benchmark
        PATHS ${_gbenchmark_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "lib" )
else ()
    find_path( GBENCHMARK_INCLUDE_DIR NAMES benchmark/benchmark.hh )
    find_library( GBENCHMARK_LIBRARIES NAMES benchmark )
endif ()


if (GBENCHMARK_INCLUDE_DIR AND GBENCHMARK_LIBRARIES)
  set(GBENCHMARK_FOUND TRUE)
  get_filename_component( GBENCHMARK_LIBS ${GBENCHMARK_LIBRARIES} PATH )
  set(GBENCHMARK_LIB_NAME libbenchmark)
  set(GBENCHMARK_STATIC_LIB ${GBENCHMARK_LIBS}/${GBENCHMARK_LIB_NAME}.a)
else ()
  set(GBENCHMARK_FOUND FALSE)
endif ()

if (GBENCHMARK_FOUND)
  if (NOT GBenchmark_FIND_QUIETLY)
    message(STATUS "Found the GBenchmark library: ${GBENCHMARK_LIBRARIES}")
  endif ()
else ()
  if (NOT GBenchmark_FIND_QUIETLY)
    set(GBENCHMARK_ERR_MSG "Could not find the GBenchmark library. Looked in ")
    if ( _gbenchmark_roots )
      set(GBENCHMARK_ERR_MSG "${GBENCHMARK_ERR_MSG} in ${_gbenchmark_roots}.")
    else ()
      set(GBENCHMARK_ERR_MSG "${GBENCHMARK_ERR_MSG} system search paths.")
    endif ()
    if (GBenchmark_FIND_REQUIRED)
      message(FATAL_ERROR "${GBENCHMARK_ERR_MSG}")
    else (GBenchmark_FIND_REQUIRED)
      message(STATUS "${GBENCHMARK_ERR_MSG}")
    endif (GBenchmark_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  GBENCHMARK_INCLUDE_DIR
  GBENCHMARK_LIBS
  GBENCHMARK_LIBRARIES
  GBENCHMARK_STATIC_LIB
)
