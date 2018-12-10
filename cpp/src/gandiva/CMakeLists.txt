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

find_package(LLVM)

# For "make gandiva" to build everything Gandiva-related
add_custom_target(gandiva)

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

set(GANDIVA_SHARED_PRIVATE_LINK_LIBS
  arrow_shared
  LLVM::LLVM_INTERFACE
  ${RE2_LIBRARY})

set(GANDIVA_STATIC_LINK_LIBS
  arrow_static
  LLVM::LLVM_INTERFACE
  ${RE2_LIBRARY})

if (ARROW_GANDIVA_STATIC_LIBSTDCPP
    AND (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX))
  set(GANDIVA_STATIC_LINK_LIBS
    ${GANDIVA_STATIC_LINK_LIBS}
    -static-libstdc++
    -static-libgcc)
endif()

ADD_ARROW_LIB(gandiva
  SOURCES ${SRC_FILES}
  OUTPUTS GANDIVA_LIBRARIES
  DEPENDENCIES arrow_dependencies precompiled
  EXTRA_INCLUDES
  $<TARGET_PROPERTY:LLVM::LLVM_INTERFACE,INTERFACE_INCLUDE_DIRECTORIES>
  SHARED_LINK_LIBS arrow_shared
  SHARED_PRIVATE_LINK_LIBS ${GANDIVA_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${GANDIVA_STATIC_LINK_LIBS})

add_dependencies(gandiva ${GANDIVA_LIBRARIES})

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
  func_descriptor.h
  function_signature.h
  gandiva_aliases.h
  literal_holder.h
  logging.h
  node.h
  node_visitor.h
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

set(GANDIVA_STATIC_TEST_LINK_LIBS
  gandiva_static
  ${RE2_LIBRARY}
  ${ARROW_TEST_LINK_LIBS})
set(GANDIVA_SHARED_TEST_LINK_LIBS
  gandiva_shared
  ${RE2_LIBRARY}
  ${ARROW_TEST_LINK_LIBS})

function(ADD_GANDIVA_TEST REL_TEST_NAME)
  set(options USE_STATIC_LINKING)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "${multi_value_args}" ${ARGN})

  set(TEST_ARGUMENTS
    ENABLED
    PREFIX "gandiva"
    LABELS "unittest;gandiva"
    ${ARG_UNPARSED_ARGUMENTS})

  # and uses less disk space, but in some cases we need to force static
  # linking (see rationale below).
  if (ARG_USE_STATIC_LINKING)
    ADD_ARROW_TEST(${REL_TEST_NAME}
      ${TEST_ARGUMENTS}
      STATIC_LINK_LIBS ${GANDIVA_STATIC_TEST_LINK_LIBS})
  else()
    ADD_ARROW_TEST(${REL_TEST_NAME}
      ${TEST_ARGUMENTS}
      STATIC_LINK_LIBS ${GANDIVA_SHARED_TEST_LINK_LIBS})
  endif()

  if(${REL_TEST_NAME} MATCHES "llvm" OR
     ${REL_TEST_NAME} MATCHES "expression_registry")
    # If the unit test has llvm in its name, include llvm.
    add_dependencies(gandiva-${REL_TEST_NAME} LLVM::LLVM_INTERFACE)
    target_link_libraries(gandiva-${REL_TEST_NAME} PRIVATE LLVM::LLVM_INTERFACE)
  endif()
endfunction()

if (ARROW_GANDIVA_BUILD_TESTS)
  ADD_GANDIVA_TEST(bitmap_accumulator_test)
  ADD_GANDIVA_TEST(engine_llvm_test)
  ADD_GANDIVA_TEST(function_signature_test)
  ADD_GANDIVA_TEST(function_registry_test)
  ADD_GANDIVA_TEST(llvm_types_test)
  ADD_GANDIVA_TEST(llvm_generator_test)
  ADD_GANDIVA_TEST(annotator_test)
  ADD_GANDIVA_TEST(tree_expr_test)
  ADD_GANDIVA_TEST(expr_decomposer_test)
  ADD_GANDIVA_TEST(expression_registry_test)
  ADD_GANDIVA_TEST(selection_vector_test)
  ADD_GANDIVA_TEST(lru_cache_test)
  ADD_GANDIVA_TEST(to_date_holder_test)
  ADD_GANDIVA_TEST(simple_arena_test)
endif()

if (ARROW_GANDIVA_JAVA)
  add_subdirectory(jni)
endif()
add_subdirectory(precompiled)

if (ARROW_GANDIVA_BUILD_TESTS)
  include(CTest)
  enable_testing()

  add_subdirectory(tests)
endif()
