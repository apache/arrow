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
#
# Usage of this module as follows:
#
#  find_package(LLVMAlt)

set(LLVM_HINTS ${LLVM_ROOT} ${LLVM_DIR} /usr/lib /usr/share)
if(LLVM_BREW_PREFIX)
  list(APPEND LLVM_HINTS ${LLVM_BREW_PREFIX})
endif()
foreach(ARROW_LLVM_VERSION ${ARROW_LLVM_VERSIONS})
  find_package(LLVM
               ${ARROW_LLVM_VERSION}
               CONFIG
               HINTS
               ${LLVM_HINTS})
  if(LLVM_FOUND)
    break()
  endif()
endforeach()

if(LLVM_FOUND)
  # Find the libraries that correspond to the LLVM components
  llvm_map_components_to_libnames(LLVM_LIBS
                                  core
                                  mcjit
                                  native
                                  ipo
                                  bitreader
                                  target
                                  linker
                                  analysis
                                  debuginfodwarf)

  find_program(LLVM_LINK_EXECUTABLE llvm-link HINTS ${LLVM_TOOLS_BINARY_DIR})

  find_program(CLANG_EXECUTABLE
               NAMES clang-${LLVM_PACKAGE_VERSION}
                     clang-${LLVM_VERSION_MAJOR}.${LLVM_VERSION_MINOR}
                     clang-${LLVM_VERSION_MAJOR} clang
               HINTS ${LLVM_TOOLS_BINARY_DIR})

  add_library(LLVM::LLVM_INTERFACE INTERFACE IMPORTED)

  set_target_properties(LLVM::LLVM_INTERFACE
                        PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                   "${LLVM_INCLUDE_DIRS}"
                                   INTERFACE_COMPILE_FLAGS
                                   "${LLVM_DEFINITIONS}"
                                   INTERFACE_LINK_LIBRARIES
                                   "${LLVM_LIBS}")
endif()

mark_as_advanced(CLANG_EXECUTABLE LLVM_LINK_EXECUTABLE)

find_package_handle_standard_args(LLVMAlt
                                  REQUIRED_VARS # The first variable is used for display.
                                  LLVM_PACKAGE_VERSION
                                  CLANG_EXECUTABLE
                                  LLVM_FOUND
                                  LLVM_LINK_EXECUTABLE)
if(LLVMAlt_FOUND)
  message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
  message(STATUS "Found llvm-link ${LLVM_LINK_EXECUTABLE}")
  message(STATUS "Found clang ${CLANG_EXECUTABLE}")
endif()
