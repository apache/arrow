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

project(gandiva)

set(CLANG_EXECUTABLE ${LLVM_TOOLS_BINARY_DIR}/clang)
set(LINK_EXECUTABLE ${LLVM_TOOLS_BINARY_DIR}/llvm-link)

set(PRECOMPILED_SRCS
    arithmetic_ops.cc
    bitmap.cc
    time.cc
    print.cc
    sample.cc)

# Create bitcode for each of the source files.
foreach(SRC_FILE ${PRECOMPILED_SRCS})
  get_filename_component(SRC_BASE ${SRC_FILE} NAME_WE)
  get_filename_component(ABSOLUTE_SRC ${SRC_FILE} ABSOLUTE)
  set(BC_FILE ${CMAKE_CURRENT_BINARY_DIR}/${SRC_BASE}.bc)
  add_custom_command(
    OUTPUT ${BC_FILE}
    COMMAND ${CLANG_EXECUTABLE}
            -std=c++11 -emit-llvm -O2 -c ${ABSOLUTE_SRC} -o ${BC_FILE}
    DEPENDS ${SRC_FILE})
  list(APPEND BC_FILES ${BC_FILE})
endforeach()

# link all of the bitcode files into a single bitcode file.
add_custom_command(
  OUTPUT ${GANDIVA_BC_OUTPUT_PATH}
  COMMAND ${LINK_EXECUTABLE}
          -o ${GANDIVA_BC_OUTPUT_PATH}
          ${BC_FILES}
  DEPENDS ${BC_FILES})

add_custom_target(precompiled ALL DEPENDS ${GANDIVA_BC_OUTPUT_PATH})

# testing
add_precompiled_unit_test(bitmap_test.cc bitmap.cc)
add_precompiled_unit_test(time_test.cc time.cc)
add_precompiled_unit_test(sample_test.cc sample.cc)
