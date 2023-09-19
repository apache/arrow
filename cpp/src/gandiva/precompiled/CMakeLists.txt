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

include(../cmake/GenerateBitcode.cmake)

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

generate_bitcode("${PRECOMPILED_SRCS}" "" BC_FILES)

# link all of the bitcode files into a single bitcode file.
add_custom_command(OUTPUT ${GANDIVA_PRECOMPILED_BC_PATH}
                   COMMAND ${LLVM_LINK_EXECUTABLE} -o ${GANDIVA_PRECOMPILED_BC_PATH}
                           ${BC_FILES}
                   DEPENDS ${BC_FILES})

# turn the bitcode file into a C++ static data variable.
add_custom_command(OUTPUT ${GANDIVA_PRECOMPILED_CC_PATH}
                   COMMAND ${PYTHON_EXECUTABLE}
                           "${CMAKE_CURRENT_SOURCE_DIR}/../make_precompiled_bitcode.py"
                           ${GANDIVA_PRECOMPILED_CC_IN_PATH}
                           ${GANDIVA_PRECOMPILED_BC_PATH} ${GANDIVA_PRECOMPILED_CC_PATH}
                   DEPENDS ${GANDIVA_PRECOMPILED_CC_IN_PATH}
                           ${GANDIVA_PRECOMPILED_BC_PATH})

add_custom_target(precompiled ALL DEPENDS ${GANDIVA_PRECOMPILED_BC_PATH}
                                          ${GANDIVA_PRECOMPILED_CC_PATH})

# testing
if(ARROW_BUILD_TESTS)
  add_executable(gandiva-precompiled-test
                 ../context_helper.cc
                 bitmap_test.cc
                 bitmap.cc
                 epoch_time_point_test.cc
                 time_test.cc
                 time.cc
                 timestamp_arithmetic.cc
                 ../cast_time.cc
                 ../../arrow/vendored/datetime/tz.cpp
                 hash_test.cc
                 hash.cc
                 string_ops_test.cc
                 string_ops.cc
                 arithmetic_ops_test.cc
                 arithmetic_ops.cc
                 extended_math_ops_test.cc
                 extended_math_ops.cc
                 decimal_ops_test.cc
                 decimal_ops.cc
                 ../decimal_type_util.cc
                 ../decimal_xlarge.cc)
  target_include_directories(gandiva-precompiled-test PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(gandiva-precompiled-test PRIVATE ${ARROW_TEST_LINK_LIBS})
  target_compile_definitions(gandiva-precompiled-test PRIVATE GANDIVA_UNIT_TEST=1
                                                              ARROW_STATIC GANDIVA_STATIC)
  set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/gandiva-precompiled-test")
  add_test(gandiva-precompiled-test ${TEST_PATH})
  set_property(TEST gandiva-precompiled-test
               APPEND
               PROPERTY LABELS "unittest;gandiva-tests")
  add_dependencies(gandiva-tests gandiva-precompiled-test)
endif()
