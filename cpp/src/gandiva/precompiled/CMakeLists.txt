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

project(gandiva)

set(PRECOMPILED_SRCS
    arithmetic_ops.cc
    bitmap.cc
    decimal_ops.cc
    decimal_wrapper.cc
    extended_math_ops.cc
    hash.cc
    print.cc
    string_ops.cc
    time.cc
    timestamp_arithmetic.cc
    ../../arrow/util/basic_decimal.cc)

if(MSVC)
  # clang pretends to be a particular version of MSVC. Version 1900 is
  # Visual Studio 2015, and the standard library uses C++14 features,
  # so we have to use that -std version to get the IR compilation to
  # work
  set(PLATFORM_CLANG_OPTIONS -std=c++14 -fms-compatibility -fms-compatibility-version=19)
else()
  set(PLATFORM_CLANG_OPTIONS -std=c++11)
endif()

# Create bitcode for each of the source files.
foreach(SRC_FILE ${PRECOMPILED_SRCS})
  get_filename_component(SRC_BASE ${SRC_FILE} NAME_WE)
  get_filename_component(ABSOLUTE_SRC ${SRC_FILE} ABSOLUTE)
  set(BC_FILE ${CMAKE_CURRENT_BINARY_DIR}/${SRC_BASE}.bc)
  add_custom_command(
    OUTPUT ${BC_FILE}
    COMMAND ${CLANG_EXECUTABLE}
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
    DEPENDS ${SRC_FILE})
  list(APPEND BC_FILES ${BC_FILE})
endforeach()

# link all of the bitcode files into a single bitcode file.
add_custom_command(OUTPUT ${GANDIVA_PRECOMPILED_BC_PATH}
                   COMMAND ${LLVM_LINK_EXECUTABLE} -o ${GANDIVA_PRECOMPILED_BC_PATH}
                           ${BC_FILES}
                   DEPENDS ${BC_FILES})

# write a cmake script to replace precompiled bitcode file's content into a .cc file
file(WRITE "${CMAKE_CURRENT_BINARY_DIR}/WritePrecompiledCC.cmake" "\
  file(READ \"${GANDIVA_PRECOMPILED_BC_PATH}\" HEXCONTENT HEX)
  string(REGEX REPLACE
         \"([0-9a-f][0-9a-f])\"
         \"'\\\\\\\\x\\\\1',\"
         GANDIVA_PRECOMPILED_BITCODE_CHARS
         \"\${HEXCONTENT}\")
  configure_file(${GANDIVA_PRECOMPILED_CC_IN_PATH}
                 ${GANDIVA_PRECOMPILED_CC_PATH})
")

# add the previous command to the execution chain
add_custom_command(OUTPUT ${GANDIVA_PRECOMPILED_CC_PATH}
                   COMMAND ${CMAKE_COMMAND} -P
                           "${CMAKE_CURRENT_BINARY_DIR}/WritePrecompiledCC.cmake"
                   DEPENDS ${GANDIVA_PRECOMPILED_CC_IN_PATH}
                           ${GANDIVA_PRECOMPILED_BC_PATH})

add_custom_target(precompiled ALL
                  DEPENDS ${GANDIVA_PRECOMPILED_BC_PATH} ${GANDIVA_PRECOMPILED_CC_PATH})

function(add_precompiled_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  set(TEST_NAME "gandiva-precompiled-${TEST_NAME}")

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_NAME} PRIVATE ${ARROW_TEST_LINK_LIBS} ${RE2_LIBRARY})
  target_compile_definitions(${TEST_NAME}
                             PRIVATE
                             GANDIVA_UNIT_TEST=1
                             ARROW_STATIC
                             GANDIVA_STATIC)
  set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")
  add_test(${TEST_NAME} ${TEST_PATH})
  set_property(TEST ${TEST_NAME} APPEND PROPERTY LABELS "unittest;gandiva-tests")
  add_dependencies(gandiva-tests ${TEST_NAME})
endfunction(add_precompiled_unit_test REL_TEST_NAME)

# testing
if(ARROW_BUILD_TESTS)
  add_precompiled_unit_test(bitmap_test.cc bitmap.cc)
  add_precompiled_unit_test(epoch_time_point_test.cc)
  add_precompiled_unit_test(time_test.cc
                            time.cc
                            timestamp_arithmetic.cc
                            ../context_helper.cc
                            ../cast_time.cc
                            ../../arrow/vendored/datetime/tz.cpp)
  add_precompiled_unit_test(hash_test.cc hash.cc)
  add_precompiled_unit_test(string_ops_test.cc string_ops.cc ../context_helper.cc)
  add_precompiled_unit_test(arithmetic_ops_test.cc arithmetic_ops.cc ../context_helper.cc)
  add_precompiled_unit_test(extended_math_ops_test.cc extended_math_ops.cc
                            ../context_helper.cc)
  add_precompiled_unit_test(decimal_ops_test.cc decimal_ops.cc ../decimal_type_util.cc)
endif()
