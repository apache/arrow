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

pkg_check_modules(GLOG_PC libglog)
if (GLOG_PC_FOUND)
  set(GLOG_INCLUDE_DIR "${GLOG_PC_INCLUDEDIR}")
  list(APPEND GLOG_PC_LIBRARY_DIRS "${GLOG_PC_LIBDIR}")
  find_library(GLOG_LIB glog
    PATHS ${GLOG_PC_LIBRARY_DIRS}
    NO_DEFAULT_PATH)
elseif(GLOG_ROOT)
  find_library(GLOG_LIB
    NAMES
	  glog
    PATHS ${GLOG_ROOT} "${GLOG_ROOT}/Library"
    PATH_SUFFIXES "lib64" "lib" "bin"
    NO_DEFAULT_PATH)
  find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h
    PATHS ${GLOG_ROOT} "${GLOG_ROOT}/Library"
    NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
else()
  find_library(GLOG_LIB
    NAMES
	  glog
    PATH_SUFFIXES "lib64" "lib" "bin")
  find_path(GLOG_INCLUDE_DIR NAMES glog/logging.h
    PATH_SUFFIXES "include")
endif()

find_package_handle_standard_args(GLOG
  REQUIRED_VARS GLOG_INCLUDE_DIR GLOG_LIB)

if (GLOG_FOUND)
  add_library(GLOG::glog UNKNOWN IMPORTED)
  set_target_properties(GLOG::glog PROPERTIES
          IMPORTED_LOCATION "${GLOG_LIB}"
          INTERFACE_INCLUDE_DIRECTORIES "${GLOG_INCLUDE_DIR}"
  )
endif()
