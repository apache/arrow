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

set(REQD_LLVM_VERSION 6.0)
find_package(LLVM ${REQD_LLVM_VERSION} REQUIRED CONFIG HINTS
             /usr/local/opt/llvm
             /usr/share)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
include_directories(${LLVM_INCLUDE_DIRS})
add_definitions(${LLVM_DEFINITIONS})

find_library(ARROW_LIB arrow)
message(STATUS "Found arrow : ${ARROW_LIB}")
find_path(ARROW_INCLUDE_DIR arrow/type.h)
message(STATUS "include dir for arrow : ${ARROW_INCLUDE_DIR}")
include_directories(${ARROW_INCLUDE_DIR})

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})

include_directories(.)
include_directories(codegen)
include_directories(codegen/cex)
include_directories(codegen/dex)
include_directories(codegen/expr)
include_directories(expr)
include_directories(precompiled)
include_directories(${CMAKE_BINARY_DIR})

set(GANDIVA_SRCS
    codegen/engine.cc
    codegen/function_registry.cc
    codegen/llvm_generator.cc
    codegen/llvm_types.cc
    expr/annotator.cc
    expr/evaluator.cc
    expr/tree_expr_builder.cc
    expr/node.cc)

set(PRECOMPILED_SOURCED_FILES
    precompiled/arithmetic_ops.cc
    precompiled/bitmap.cc
    precompiled/time.cc)

SET(PRECOMPILED_SRC precompiled/irhelpers.cc)
#TODO : fix the location of the .bc file, it shouldn't be /tmp.
SET(PRECOMPILED_OUTPUT /tmp/irhelpers.bc)

find_program(CLANG_EXECUTABLE NAMES clang++-${REQD_LLVM_VERSION} clang)
message(STATUS "Found clang++ ${CLANG_EXECUTABLE}")

get_filename_component(PRECOMPILED_ABSOLUTE_SRC ${PRECOMPILED_SRC} ABSOLUTE)
add_custom_command(
  OUTPUT ${PRECOMPILED_OUTPUT}
  COMMAND ${CLANG_EXECUTABLE} -flto -O2 -c ${PRECOMPILED_ABSOLUTE_SRC} -o ${PRECOMPILED_OUTPUT}
  DEPENDS ${PRECOMPILED_SRC} ${PRECOMPILED_SOURCED_FILES})
add_custom_target(precompile-to-ir ALL DEPENDS ${PRECOMPILED_OUTPUT})

add_library(gandiva SHARED ${GANDIVA_SRCS})

# Find the libraries that correspond to the LLVM components
llvm_map_components_to_libnames(llvm_libs core mcjit native ipo bitreader target linker analysis debuginfodwarf)
# Link against LLVM libraries
target_link_libraries(gandiva ${llvm_libs})
target_link_libraries(gandiva ${ARROW_LIB})

# Test related

set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)

function(ADD_GANDIVA_TEST LABEL REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)
  set(TEST_PATH "${EXECUTABLE_OUTPUT_PATH}/${TEST_NAME}")

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  if(${LABEL} STREQUAL "integ")
    target_link_libraries(${TEST_NAME} gandiva)
  endif()
  if(${REL_TEST_NAME} MATCHES "llvm")
    target_link_libraries(${TEST_NAME} ${llvm_libs})
  endif()
  target_link_libraries(${TEST_NAME} ${ARROW_LIB})
  target_link_libraries(${TEST_NAME} ${GTEST_BOTH_LIBRARIES})
  target_link_libraries(${TEST_NAME} Threads::Threads)
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS ${LABEL} ${TEST_NAME})
endfunction()

#args: label test-file src-files
ADD_GANDIVA_TEST(unittest codegen/dex_llvm_test.cc)
ADD_GANDIVA_TEST(unittest codegen/engine_llvm_test.cc codegen/engine.cc codegen/llvm_types.cc)
ADD_GANDIVA_TEST(unittest codegen/function_signature_test.cc)
ADD_GANDIVA_TEST(unittest codegen/function_registry_test.cc codegen/function_registry.cc)
ADD_GANDIVA_TEST(unittest codegen/llvm_types_test.cc codegen/llvm_types.cc)
ADD_GANDIVA_TEST(unittest codegen/llvm_generator_test.cc codegen/llvm_generator.cc codegen/engine.cc codegen/llvm_types.cc codegen/function_registry.cc expr/annotator.cc)
ADD_GANDIVA_TEST(unittest expr/annotator_test.cc expr/annotator.cc)
ADD_GANDIVA_TEST(unittest expr/tree_expr_test.cc expr/tree_expr_builder.cc expr/node.cc expr/annotator.cc codegen/function_registry.cc)

ADD_GANDIVA_TEST(integ expr/evaluator_test.cc)
