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
# Tries to find GLog headers and libraries.
#
# Usage of this module as follows:
#
#  find_package(GLOG)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  GLOG_HOME - When set, this path is inspected instead of standard library
#                locations as the root of the GLog installation.
#                The environment variable GLOG_HOME overrides this veriable.
#
# This module defines
#  GLOG_INCLUDE_DIR, directory containing headers
#  GLOG_STATIC_LIB, path to libglog.a
#  GLOG_FOUND, whether glog has been found

if( NOT "${GLOG_HOME}" STREQUAL "")
    file( TO_CMAKE_PATH "${GLOG_HOME}" _native_path )
    list( APPEND _glog_roots ${_native_path} )
endif()

message(STATUS "GLOG_HOME: ${GLOG_HOME}")
# Try the parameterized roots, if they exist
if ( _glog_roots )
    set (lib_dirs "lib")
    if (EXISTS "${_glog_roots}/lib64")
      set (lib_dirs "lib64" ${lib_dirs})
    endif ()

    find_path( GLOG_INCLUDE_DIR NAMES glog/logging.h
        PATHS ${_glog_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES "include" )
    find_library( GLOG_LIBRARIES NAMES glog
        PATHS ${_glog_roots} NO_DEFAULT_PATH
        PATH_SUFFIXES ${lib_dirs})
else ()
    find_path( GLOG_INCLUDE_DIR NAMES glog/logging.h )
    find_library( GLOG_LIBRARIES NAMES glog )
endif ()


if (GLOG_INCLUDE_DIR AND GLOG_LIBRARIES)
  set(GLOG_FOUND TRUE)
  get_filename_component( GLOG_LIBS ${GLOG_LIBRARIES} PATH )
  set(GLOG_LIB_NAME glog)
  set(GLOG_STATIC_LIB ${GLOG_LIBS}/${CMAKE_STATIC_LIBRARY_PREFIX}${GLOG_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX})
  set(GLOG_SHARED_LIB ${GLOG_LIBS}/${CMAKE_SHARED_LIBRARY_PREFIX}${GLOG_LIB_NAME}${CMAKE_SHARED_LIBRARY_SUFFIX})
else ()
  set(GLOG_FOUND FALSE)
endif ()

if (GLOG_FOUND)
  if (NOT GLOG_FIND_QUIETLY)
    message(STATUS "Found the GLog library: ${GLOG_LIBRARIES}")
  endif ()
else ()
  if (NOT GLOG_FIND_QUIETLY)
    set(GLOG_ERR_MSG "Could not find the GLog library. Looked in ")
    if ( _glog_roots )
      set(GLOG_ERR_MSG "${GLOG_ERR_MSG} ${_glog_roots}.")
    else ()
      set(GLOG_ERR_MSG "${GLOG_ERR_MSG} system search paths.")
    endif ()
    if (GLOG_FIND_REQUIRED)
      message(FATAL_ERROR "${GLOG_ERR_MSG}")
    else (GLOG_FIND_REQUIRED)
      message(STATUS "${GLOG_ERR_MSG}")
    endif (GLOG_FIND_REQUIRED)
  endif ()
endif ()

mark_as_advanced(
  GLOG_INCLUDE_DIR
  GLOG_LIBS
  GLOG_LIBRARIES
  GLOG_STATIC_LIB
)
