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
#  find_package(LLVM)
#

if(APPLE)
  # Also look in homebrew for a matching llvm version
  find_program(BREW_BIN brew)
  if(BREW_BIN)
    execute_process(COMMAND ${BREW_BIN} --prefix "llvm@7"
                    OUTPUT_VARIABLE LLVM_BREW_PREFIX
                    OUTPUT_STRIP_TRAILING_WHITESPACE)
  endif()
endif()

set(LLVM_HINTS
    ${LLVM_ROOT}
    ${LLVM_DIR}
    /usr/lib
    /usr/local/opt/llvm
    /usr/share
    ${LLVM_BREW_PREFIX})
if(DEFINED ARROW_LLVM_VERSION_FALLBACK)
  find_package(LLVM
               ${ARROW_LLVM_VERSION}
               CONFIG
               HINTS
               ${LLVM_HINTS})
  if(NOT ${LLVM_FOUND})
    find_package(LLVM
                 ${ARROW_LLVM_VERSION_FALLBACK}
                 REQUIRED
                 CONFIG
                 HINTS
                 ${LLVM_HINTS})
  endif()
else()
  find_package(LLVM
               ${ARROW_LLVM_VERSION}
               REQUIRED
               CONFIG
               HINTS
               ${LLVM_HINTS})
endif()
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")

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
if(LLVM_LINK_EXECUTABLE)
  message(STATUS "Found llvm-link ${LLVM_LINK_EXECUTABLE}")
else()
  message(FATAL_ERROR "Couldn't find llvm-link")
endif()

find_program(CLANG_EXECUTABLE
             NAMES clang-${ARROW_LLVM_VERSION} clang-${ARROW_LLVM_MAJOR_VERSION}
             HINTS ${LLVM_TOOLS_BINARY_DIR})
if(CLANG_EXECUTABLE)
  message(STATUS "Found clang ${LLVM_PACKAGE_VERSION} ${CLANG_EXECUTABLE}")
else()
  # If clang-7 not available, switch to normal clang.
  find_program(CLANG_EXECUTABLE NAMES clang HINTS ${LLVM_TOOLS_BINARY_DIR})
  if(CLANG_EXECUTABLE)
    message(STATUS "Found clang ${CLANG_EXECUTABLE}")
  else()
    message(FATAL_ERROR "Couldn't find clang")
  endif()
endif()

add_library(LLVM::LLVM_INTERFACE INTERFACE IMPORTED)

set_target_properties(LLVM::LLVM_INTERFACE
                      PROPERTIES INTERFACE_INCLUDE_DIRECTORIES
                                 "${LLVM_INCLUDE_DIRS}"
                                 INTERFACE_COMPILE_FLAGS
                                 "${LLVM_DEFINITIONS}"
                                 INTERFACE_LINK_LIBRARIES
                                 "${LLVM_LIBS}")

mark_as_advanced(CLANG_EXECUTABLE LLVM_LINK_EXECUTABLE)
