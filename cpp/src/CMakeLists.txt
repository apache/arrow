# Copyright (C) 2017-2018 Dremio Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.10)

project(gandiva)

# LLVM/Clang is required by multiple subdirs.
find_package(LLVM)

# Set the path where the byte-code files will be installed.
set(GANDIVA_BC_INSTALL_DIR
  ${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_INCLUDEDIR}/gandiva)

set(GANDIVA_BC_FILE_NAME irhelpers.bc)
set(GANDIVA_BC_INSTALL_PATH ${GANDIVA_BC_INSTALL_DIR}/${GANDIVA_BC_FILE_NAME})
set(GANDIVA_BC_OUTPUT_PATH ${CMAKE_BINARY_DIR}/${GANDIVA_BC_FILE_NAME})

# Set the path where the so lib file will be installed.
if (APPLE)
  set(GANDIVA_HELPER_LIB_FILE_NAME libgandiva_helpers.dylib)
else()
  set(GANDIVA_HELPER_LIB_FILE_NAME libgandiva_helpers.so)
endif(APPLE)

set(GANDIVA_HELPER_LIB_INSTALL_PATH ${GANDIVA_BC_INSTALL_DIR}/${GANDIVA_HELPER_LIB_FILE_NAME})
set(GANDIVA_HELPER_LIB_OUTPUT_PATH ${CMAKE_BINARY_DIR}/src/codegen/${GANDIVA_HELPER_LIB_FILE_NAME})

add_subdirectory(codegen)
add_subdirectory(jni)
add_subdirectory(precompiled)
