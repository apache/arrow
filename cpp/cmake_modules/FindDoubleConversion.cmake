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

if(DoubleConversion_ROOT)
  find_library(DoubleConversion_LIB
    NAMES double-conversion
    PATHS ${DoubleConversion_ROOT}
    NO_DEFAULT_PATH)
  find_path(DoubleConversion_INCLUDE_DIR NAMES double-conversion/double-conversion.h
    PATHS ${DoubleConversion_ROOT} NO_DEFAULT_PATH
    PATH_SUFFIXES "include")
else()
  find_library(DoubleConversion_LIB
    NAMES double-conversion)
  find_path(DoubleConversion_INCLUDE_DIR NAMES double-conversion/double-conversion.h
    PATH_SUFFIXES "include")
endif()

find_package_handle_standard_args(DoubleConversion
  REQUIRED_VARS DoubleConversion_LIB DoubleConversion_INCLUDE_DIR)

if (DoubleConversion_FOUND)
  add_library(double-conversion::double-conversion UNKNOWN IMPORTED)
  set_target_properties(double-conversion::double-conversion PROPERTIES
          IMPORTED_LOCATION "${DoubleConversion_LIB}"
          INTERFACE_INCLUDE_DIRECTORIES "${DoubleConversion_INCLUDE_DIR}"
  )
endif()
