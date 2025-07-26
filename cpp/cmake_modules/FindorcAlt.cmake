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

if(orcAlt_FOUND)
  return()
endif()

set(find_package_args)
if(orcAlt_FIND_VERSION)
  list(APPEND find_package_args ${orcAlt_FIND_VERSION})
endif()
if(orcAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(orc ${find_package_args})
if(orc_FOUND)
  set(orcAlt_FOUND TRUE)
  set(orcAlt_VERSION ${orc_VERSION})
  return()
endif()

if(ORC_ROOT)
  find_library(ORC_STATIC_LIB
               NAMES orc
               PATHS ${ORC_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATHS ${ORC_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
else()
  find_library(ORC_STATIC_LIB
               NAMES orc
               PATH_SUFFIXES ${ARROW_LIBRARY_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATH_SUFFIXES ${ARROW_INCLUDE_PATH_SUFFIXES})
endif()
if(ORC_INCLUDE_DIR)
  file(READ "${ORC_INCLUDE_DIR}/orc/orc-config.hh" ORC_CONFIG_HH_CONTENT)
  string(REGEX MATCH "#define ORC_VERSION \"[0-9.]+\"" ORC_VERSION_DEFINITION
               "${ORC_CONFIG_HH_CONTENT}")
  string(REGEX MATCH "[0-9.]+" ORC_VERSION "${ORC_VERSION_DEFINITION}")
endif()

find_package_handle_standard_args(
  orcAlt
  REQUIRED_VARS ORC_STATIC_LIB ORC_INCLUDE_DIR
  VERSION_VAR ORC_VERSION)

if(orcAlt_FOUND)
  if(NOT TARGET orc::orc)
    # For old Apache Orc. For example, apache-orc 2.0.3 on Alpine
    # Linux 3.20 and 3.21.
    add_library(orc::orc STATIC IMPORTED)
    set_target_properties(orc::orc
                          PROPERTIES IMPORTED_LOCATION "${ORC_STATIC_LIB}"
                                     INTERFACE_INCLUDE_DIRECTORIES "${ORC_INCLUDE_DIR}")
    if(ARROW_WITH_LZ4 AND TARGET LZ4::lz4)
      target_link_libraries(orc::orc INTERFACE LZ4::lz4)
    endif()
    if(ARROW_WITH_SNAPPY AND TARGET Snappy::snappy)
      target_link_libraries(orc::orc INTERFACE Snappy::snappy)
    endif()
    if(ARROW_WITH_ZSTD)
      if(TARGET zstd::libzstd_shared)
        target_link_libraries(orc::orc INTERFACE zstd::libzstd_shared)
      elseif(TARGET zstd::libzstd_static)
        target_link_libraries(orc::orc INTERFACE zstd::libzstd_static)
      endif()
    endif()
    if(ARROW_WITH_ZLIB AND TARGET ZLIB::ZLIB)
      target_link_libraries(orc::orc INTERFACE ZLIB::ZLIB)
    endif()
  endif()
  set(orcAlt_VERSION ${ORC_VERSION})
endif()
