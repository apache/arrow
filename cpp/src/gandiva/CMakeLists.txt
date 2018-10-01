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

# LLVM/Clang is required by multiple subdirs.
cmake_minimum_required(VERSION 3.11)

project(gandiva)

include(GandivaBuildUtils)

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
set(GANDIVA_HELPER_LIB_OUTPUT_PATH ${CMAKE_BINARY_DIR}/debug/${GANDIVA_HELPER_LIB_FILE_NAME})

set(BC_FILE_PATH_CC "${CMAKE_CURRENT_BINARY_DIR}/bc_file_path.cc")
configure_file(bc_file_path.cc.in ${BC_FILE_PATH_CC})

# helper files that are shared between libgandiva and libgandiva_helpers
set(SHARED_HELPER_FILES
      like_holder.cc
      regex_util.cc
      execution_context.cc
      to_date_holder.cc
      date_utils.cc)

set(SRC_FILES annotator.cc
      bitmap_accumulator.cc
      configuration.cc
      engine.cc
      expr_decomposer.cc
      expr_validator.cc
      expression.cc
      expression_registry.cc
      filter.cc
      function_registry.cc
      function_signature.cc
      llvm_generator.cc
      llvm_types.cc
      projector.cc
      selection_vector.cc
      tree_expr_builder.cc
      ${SHARED_HELPER_FILES}
      ${BC_FILE_PATH_CC})

# TODO(wesm): Replace this with ADD_ARROW_LIB which deals with objlib/no-objlib
# (not supported on Windows), dependencies, etc.
add_library(gandiva_obj_lib OBJECT ${SRC_FILES})
add_dependencies(gandiva_obj_lib arrow_dependencies)

# set PIC so that object library can be included in shared libs.
set_target_properties(gandiva_obj_lib PROPERTIES POSITION_INDEPENDENT_CODE 1)

# For users of gandiva library (including integ tests), include-dir is :
#   /usr/**/include dir after install,
#   cpp/include during build
# For building gandiva library itself, include-dir (in addition to above) is :
#   cpp/src
target_include_directories(gandiva_obj_lib
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>)

target_include_directories(gandiva_obj_lib SYSTEM
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    $<TARGET_PROPERTY:LLVM::LLVM_INTERFACE,INTERFACE_INCLUDE_DIRECTORIES>
    ${ARROW_INCLUDE_DIR}
    $<TARGET_PROPERTY:Boost::boost,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:gtest_main,INCLUDE_DIRECTORIES>
)

build_gandiva_lib("shared" arrow_shared)

build_gandiva_lib("static" arrow_static)

# Pre-compiled .so library for function helpers.
add_library(gandiva_helpers SHARED
  ${SHARED_HELPER_FILES}
  function_holder_stubs.cc)

add_dependencies(gandiva_helpers arrow_dependencies)

target_compile_definitions(gandiva_helpers
  PRIVATE -DGDV_HELPERS
)

target_include_directories(gandiva_helpers
  PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
    ${ARROW_INCLUDE_DIR}
)

target_link_libraries(gandiva_helpers PRIVATE Boost::boost re2)

# hide all symbols that are not needed.
if (NOT APPLE)
  # apple linker does not support version scripts, not needed since we package from travis.
  set_target_properties(gandiva_helpers PROPERTIES LINK_FLAGS "-Wl,--version-script=${CMAKE_SOURCE_DIR}/src/gandiva/symbols-helpers.map")
  target_link_libraries(gandiva_helpers LINK_PRIVATE -static-libstdc++ -static-libgcc)
endif()

# install for gandiva
include(GNUInstallDirs)

# install libgandiva
install(
  TARGETS gandiva_shared gandiva_static gandiva_helpers
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# install the header files.
install(FILES
  arrow.h
  condition.h
  configuration.h
  expression.h
  expression_registry.h
  filter.h
  function_signature.h
  gandiva_aliases.h
  logging.h
  projector.h
  selection_vector.h
  status.h
  tree_expr_builder.h
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

# pkg-config support
configure_file(gandiva.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/gandiva.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/gandiva.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

#args: label test-file src-files
add_gandiva_unit_test(bitmap_accumulator_test.cc bitmap_accumulator.cc execution_context.cc)
add_gandiva_unit_test(engine_llvm_test.cc engine.cc llvm_types.cc configuration.cc execution_context.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(function_signature_test.cc function_signature.cc)
add_gandiva_unit_test(function_registry_test.cc function_registry.cc function_signature.cc)
add_gandiva_unit_test(llvm_types_test.cc llvm_types.cc)
add_gandiva_unit_test(llvm_generator_test.cc llvm_generator.cc regex_util.cc engine.cc llvm_types.cc expr_decomposer.cc function_registry.cc annotator.cc bitmap_accumulator.cc configuration.cc  function_signature.cc like_holder.cc to_date_holder.cc date_utils.cc regex_util.cc execution_context.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(annotator_test.cc annotator.cc function_signature.cc)
add_gandiva_unit_test(tree_expr_test.cc tree_expr_builder.cc expr_decomposer.cc annotator.cc function_registry.cc function_signature.cc like_holder.cc regex_util.cc to_date_holder.cc date_utils.cc execution_context.cc)
add_gandiva_unit_test(expr_decomposer_test.cc expr_decomposer.cc tree_expr_builder.cc annotator.cc function_registry.cc function_signature.cc like_holder.cc regex_util.cc to_date_holder.cc date_utils.cc execution_context.cc)
add_gandiva_unit_test(status_test.cc)
add_gandiva_unit_test(expression_registry_test.cc llvm_types.cc expression_registry.cc function_signature.cc function_registry.cc)
add_gandiva_unit_test(selection_vector_test.cc selection_vector.cc)
add_gandiva_unit_test(lru_cache_test.cc)
add_gandiva_unit_test(to_date_holder_test.cc to_date_holder.cc date_utils.cc execution_context.cc)

add_subdirectory(jni)
add_subdirectory(precompiled)

include(CTest)
enable_testing()

add_subdirectory(tests)
