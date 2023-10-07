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

# Create bitcode for each of the source files.
function(generate_bitcode PRECOMPILED_SRCS OUTPUT_VAR)
  set(LOCAL_BC_FILES "")

  set(PLATFORM_CLANG_OPTIONS -std=c++17)
  if(MSVC)
    # "19.20" means that it's compatible with Visual Studio 16 2019.
    # We can update this to "19.30" when we dropped support for Visual
    # Studio 16 2019.
    #
    # See https://cmake.org/cmake/help/latest/variable/MSVC_VERSION.html
    # for MSVC_VERSION and Visual Studio version.
    set(FMS_COMPATIBILITY 19.20)
    list(APPEND PLATFORM_CLANG_OPTIONS -fms-compatibility
         -fms-compatibility-version=${FMS_COMPATIBILITY})
  endif()

  # Create bitcode for each of the source files.
  foreach(SRC_FILE ${PRECOMPILED_SRCS})
    get_filename_component(SRC_BASE ${SRC_FILE} NAME_WE)
    get_filename_component(ABSOLUTE_SRC ${SRC_FILE} ABSOLUTE)
    set(BC_FILE ${CMAKE_CURRENT_BINARY_DIR}/${SRC_BASE}.bc)
    set(PRECOMPILE_COMMAND)
    if(CMAKE_OSX_SYSROOT)
      list(APPEND
           PRECOMPILE_COMMAND
           ${CMAKE_COMMAND}
           -E
           env
           SDKROOT=${CMAKE_OSX_SYSROOT})
    endif()
    list(APPEND
         PRECOMPILE_COMMAND
         ${CLANG_EXECUTABLE}
         ${PLATFORM_CLANG_OPTIONS}
         -DGANDIVA_IR
         -DNDEBUG # DCHECK macros not implemented in precompiled code
         -DARROW_STATIC # Do not set __declspec(dllimport) on MSVC on Arrow symbols
         -DGANDIVA_STATIC # Do not set __declspec(dllimport) on MSVC on Gandiva symbols
         -fno-use-cxa-atexit # Workaround for unresolved __dso_handle
         -emit-llvm
         -O3
         -c
         ${ABSOLUTE_SRC}
         -o
         ${BC_FILE}
         ${ARROW_GANDIVA_PC_CXX_FLAGS}
         -I${CMAKE_SOURCE_DIR}/src
         -I${ARROW_BINARY_DIR}/src)

    if(NOT ARROW_USE_NATIVE_INT128)
      foreach(boost_include_dir ${Boost_INCLUDE_DIRS})
        list(APPEND PRECOMPILE_COMMAND -I${boost_include_dir})
      endforeach()
    endif()
    add_custom_command(OUTPUT ${BC_FILE}
                       COMMAND ${PRECOMPILE_COMMAND}
                       DEPENDS ${SRC_FILE})
    list(APPEND LOCAL_BC_FILES ${BC_FILE})
  endforeach()

  set(${OUTPUT_VAR}
      "${LOCAL_BC_FILES}"
      PARENT_SCOPE)
endfunction()
