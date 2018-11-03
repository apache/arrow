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
set(GANDIVA_BC_OUTPUT_PATH ${BUILD_OUTPUT_ROOT_DIRECTORY}/${GANDIVA_BC_FILE_NAME})

set(BC_FILE_PATH_CC "${CMAKE_CURRENT_BINARY_DIR}/bc_file_path.cc")
configure_file(bc_file_path.cc.in ${BC_FILE_PATH_CC})

set(SRC_FILES annotator.cc
      bitmap_accumulator.cc
      configuration.cc
      context_helper.cc
      engine.cc
      date_utils.cc
      expr_decomposer.cc
      expr_validator.cc
      expression.cc
      expression_registry.cc
      exported_funcs_registry.cc
      filter.cc
      function_registry.cc
      function_signature.cc
      gdv_function_stubs.cc
      llvm_generator.cc
      llvm_types.cc
      like_holder.cc
      projector.cc
      regex_util.cc
      selection_vector.cc
      tree_expr_builder.cc
      to_date_holder.cc
      ${SHARED_HELPER_FILES}
      ${BC_FILE_PATH_CC})

set(GANDIVA_PRIVATE_LINK_LIBS
  boost_regex
  boost_system
  boost_filesystem
  LLVM::LLVM_INTERFACE
  re2)
set(GANDIVA_SHARED_PRIVATE_LINK_LIBS ${GANDIVA_PRIVATE_LINK_LIBS})
set(GANDIVA_STATIC_PRIVATE_LINK_LIBS ${GANDIVA_PRIVATE_LINK_LIBS})
if (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX)
  set(GANDIVA_STATIC_PRIVATE_LINK_LIBS
    ${GANDIVA_STATIC_PRIVATE_LINK_LIBS}
    -static-libstdc++
    -static-libgcc)
endif()

add_arrow_lib(gandiva
  SOURCES ${SRC_FILES}
  OUTPUTS GANDIVA_LIBRARIES
  DEPENDENCIES arrow_dependencies
  EXTRA_INCLUDES
  $<TARGET_PROPERTY:LLVM::LLVM_INTERFACE,INTERFACE_INCLUDE_DIRECTORIES>
  SHARED_LINK_LIBS arrow_shared
  SHARED_PRIVATE_LINK_LIBS ${GANDIVA_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS arrow_static
  STATIC_PRIVATE_LINK_LIBS ${GANDIVA_STATIC_PRIVATE_LINK_LIBS})

# install for gandiva
include(GNUInstallDirs)

# install libgandiva
install(
  TARGETS gandiva_shared gandiva_static
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
  tree_expr_builder.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/gandiva")

# pkg-config support
configure_file(gandiva.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/gandiva.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/gandiva.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")

#args: label test-file src-files
add_gandiva_unit_test(bitmap_accumulator_test.cc bitmap_accumulator.cc)
add_gandiva_unit_test(engine_llvm_test.cc engine.cc llvm_types.cc configuration.cc
    gdv_function_stubs.cc context_helper.cc to_date_holder.cc date_utils.cc
    exported_funcs_registry.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(function_signature_test.cc function_signature.cc)
add_gandiva_unit_test(function_registry_test.cc function_registry.cc function_signature.cc)
add_gandiva_unit_test(llvm_types_test.cc llvm_types.cc)
add_gandiva_unit_test(llvm_generator_test.cc llvm_generator.cc regex_util.cc engine.cc
    llvm_types.cc expr_decomposer.cc function_registry.cc annotator.cc
    bitmap_accumulator.cc configuration.cc  function_signature.cc like_holder.cc
    to_date_holder.cc date_utils.cc regex_util.cc gdv_function_stubs.cc context_helper.cc
    exported_funcs_registry.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(annotator_test.cc annotator.cc function_signature.cc)
add_gandiva_unit_test(tree_expr_test.cc tree_expr_builder.cc expr_decomposer.cc annotator.cc function_registry.cc function_signature.cc like_holder.cc regex_util.cc to_date_holder.cc date_utils.cc)
add_gandiva_unit_test(expr_decomposer_test.cc expr_decomposer.cc tree_expr_builder.cc annotator.cc function_registry.cc function_signature.cc like_holder.cc regex_util.cc to_date_holder.cc date_utils.cc)
add_gandiva_unit_test(expression_registry_test.cc llvm_types.cc expression_registry.cc function_signature.cc function_registry.cc)
add_gandiva_unit_test(selection_vector_test.cc selection_vector.cc)
add_gandiva_unit_test(lru_cache_test.cc)
add_gandiva_unit_test(to_date_holder_test.cc to_date_holder.cc date_utils.cc)
add_gandiva_unit_test(simple_arena_test.cc)

if (ARROW_GANDIVA_JAVA)
  add_subdirectory(jni)
endif()
add_subdirectory(precompiled)

include(CTest)
enable_testing()

add_subdirectory(tests)
