#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License. See accompanying LICENSE file.

# -*- cmake -*-

# - Find Google perftools
# Find the Google perftools includes and libraries
# This module defines
#  GOOGLE_PERFTOOLS_INCLUDE_DIR, where to find heap-profiler.h, etc.
#  GOOGLE_PERFTOOLS_FOUND, If false, do not try to use Google perftools.
# also defined for general use are
#  TCMALLOC_LIBS, where to find the tcmalloc libraries.
#  TCMALLOC_STATIC_LIB, path to libtcmalloc.a.
#  TCMALLOC_SHARED_LIB, path to libtcmalloc's shared library
#  PROFILER_LIBS, where to find the profiler libraries.
#  PROFILER_STATIC_LIB, path to libprofiler.a.
#  PROFILER_SHARED_LIB, path to libprofiler's shared library

FIND_PATH(GOOGLE_PERFTOOLS_INCLUDE_DIR google/heap-profiler.h
  $ENV{NATIVE_TOOLCHAIN}/gperftools-$ENV{GPERFTOOLS_VERSION}/include
  NO_DEFAULT_PATH
)

SET(GPERF_LIB_SEARCH $ENV{NATIVE_TOOLCHAIN}/gperftools-$ENV{GPERFTOOLS_VERSION}/lib)

FIND_LIBRARY(TCMALLOC_LIB_PATH
  NAMES libtcmalloc.a
  PATHS ${GPERF_LIB_SEARCH}
  PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
  NO_DEFAULT_PATH
)

IF (TCMALLOC_LIB_PATH AND GOOGLE_PERFTOOLS_INCLUDE_DIR)
    SET(TCMALLOC_LIBS ${GPERF_LIB_SEARCH})
    SET(TCMALLOC_LIB_NAME libtcmalloc)
    SET(TCMALLOC_STATIC_LIB ${GPERF_LIB_SEARCH}/${TCMALLOC_LIB_NAME}.a)
    SET(TCMALLOC_SHARED_LIB ${TCMALLOC_LIBS}/${TCMALLOC_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
    SET(GOOGLE_PERFTOOLS_FOUND "YES")
ELSE (TCMALLOC_LIB_PATH AND GOOGLE_PERFTOOLS_INCLUDE_DIR)
  SET(GOOGLE_PERFTOOLS_FOUND "NO")
ENDIF (TCMALLOC_LIB_PATH AND GOOGLE_PERFTOOLS_INCLUDE_DIR)

FIND_LIBRARY(PROFILER_LIB_PATH
  NAMES libprofiler.a
  PATHS ${GPERF_LIB_SEARCH}
  PATH_SUFFIXES ${LIB_PATH_SUFFIXES}
)

IF (PROFILER_LIB_PATH AND GOOGLE_PERFTOOLS_INCLUDE_DIR)
  SET(PROFILER_LIBS ${GPERF_LIB_SEARCH})
  SET(PROFILER_LIB_NAME libprofiler)
  SET(PROFILER_STATIC_LIB ${GPERF_LIB_SEARCH}/${PROFILER_LIB_NAME}.a)
  SET(PROFILER_SHARED_LIB ${PROFILER_LIBS}/${PROFILER_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
ENDIF (PROFILER_LIB_PATH AND GOOGLE_PERFTOOLS_INCLUDE_DIR)

IF (GOOGLE_PERFTOOLS_FOUND)
  IF (NOT GPerf_FIND_QUIETLY)
    MESSAGE(STATUS "Found the Google perftools library: ${TCMALLOC_LIBS}")
  ENDIF (NOT GPerf_FIND_QUIETLY)
ELSE (GOOGLE_PERFTOOLS_FOUND)
  IF (GPerf_FIND_REQUIRED)
    MESSAGE(FATAL_ERROR "Could not find the Google perftools library")
  ENDIF (GPerf_FIND_REQUIRED)
ENDIF (GOOGLE_PERFTOOLS_FOUND)

MARK_AS_ADVANCED(
  TCMALLOC_LIBS
  TCMALLOC_STATIC_LIB
  TCMALLOC_SHARED_LIB
  PROFILER_LIBS
  PROFILER_STATIC_LIB
  PROFILER_SHARED_LIB
  GOOGLE_PERFTOOLS_INCLUDE_DIR
)
