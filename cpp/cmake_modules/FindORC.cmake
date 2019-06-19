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

# - Find Apache ORC C++ (orc/orc-config.h, liborc.a)
# This module defines
#  ORC_INCLUDE_DIR, directory containing headers
#  ORC_STATIC_LIB, path to liborc.a
#  ORC_FOUND, whether orc has been found

if(ORC_ROOT)
  find_library(ORC_STATIC_LIB
               NAMES orc
               PATHS ${ORC_ROOT}
               NO_DEFAULT_PATH
               PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATHS ${ORC_ROOT}
            NO_DEFAULT_PATH
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
else()
  find_library(ORC_STATIC_LIB NAMES orc PATH_SUFFIXES ${LIB_PATH_SUFFIXES})
  find_path(ORC_INCLUDE_DIR
            NAMES orc/orc-config.hh
            PATH_SUFFIXES ${INCLUDE_PATH_SUFFIXES})
endif()

if(ORC_STATIC_LIB AND ORC_INCLUDE_DIR)
  set(ORC_FOUND TRUE)
  add_library(orc::liborc STATIC IMPORTED)
  set_target_properties(orc::liborc
                        PROPERTIES IMPORTED_LOCATION "${ORC_STATIC_LIB}"
                                   INTERFACE_INCLUDE_DIRECTORIES
                                   "${ORC_INCLUDE_DIR}")
else()
  if (ORC_FIND_REQUIRED)
    message(FATAL_ERROR "ORC library was required in toolchain and unable to locate")
  endif()
  set(ORC_FOUND FALSE)
endif()
