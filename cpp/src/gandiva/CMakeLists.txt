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

set(GANDIVA_VERSION "${ARROW_VERSION}")

# For "make gandiva" to build everything Gandiva-related
add_custom_target(gandiva-all)
add_custom_target(gandiva)
add_custom_target(gandiva-tests)
add_custom_target(gandiva-benchmarks)

add_dependencies(gandiva-all gandiva gandiva-tests gandiva-benchmarks)

find_package(LLVM)

# Set the path where the bitcode file generated, see precompiled/CMakeLists.txt
set(GANDIVA_PRECOMPILED_BC_PATH "${CMAKE_CURRENT_BINARY_DIR}/irhelpers.bc")
set(GANDIVA_PRECOMPILED_CC_PATH "${CMAKE_CURRENT_BINARY_DIR}/precompiled_bitcode.cc")
set(GANDIVA_PRECOMPILED_CC_IN_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/precompiled_bitcode.cc.in")

# add_arrow_lib will look for this not yet existing file, so flag as generated
set_source_files_properties(${GANDIVA_PRECOMPILED_CC_PATH} PROPERTIES GENERATED TRUE)

set(SRC_FILES
    annotator.cc
    bitmap_accumulator.cc
    cast_time.cc
    configuration.cc
    context_helper.cc
    decimal_ir.cc
    decimal_type_util.cc
    engine.cc
    date_utils.cc
    expr_decomposer.cc
    expr_validator.cc
    expression.cc
    expression_registry.cc
    exported_funcs_registry.cc
    filter.cc
    function_ir_builder.cc
    function_registry.cc
    function_registry_arithmetic.cc
    function_registry_datetime.cc
    function_registry_hash.cc
    function_registry_math_ops.cc
    function_registry_string.cc
    function_registry_timestamp_arithmetic.cc
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
    ${GANDIVA_PRECOMPILED_CC_PATH})

set(GANDIVA_SHARED_PRIVATE_LINK_LIBS arrow_shared LLVM::LLVM_INTERFACE ${RE2_LIBRARY})

set(GANDIVA_STATIC_LINK_LIBS arrow_static LLVM::LLVM_INTERFACE ${RE2_LIBRARY})

if(ARROW_GANDIVA_STATIC_LIBSTDCPP
   AND (CMAKE_COMPILER_IS_GNUCC OR CMAKE_COMPILER_IS_GNUCXX))
  set(GANDIVA_STATIC_LINK_LIBS ${GANDIVA_STATIC_LINK_LIBS} -static-libstdc++
      -static-libgcc)
endif()

# if (MSVC)
#   # Symbols that need to be made public in gandiva.dll for LLVM IR
#   # compilation
#   set(MSVC_SYMBOL_EXPORTS _Init_thread_header)
#   foreach(SYMBOL ${MSVC_SYMBOL_EXPORTS})
#     set(GANDIVA_SHARED_LINK_FLAGS "${GANDIVA_SHARED_LINK_FLAGS} /EXPORT:${SYMBOL}")
#   endforeach()
# endif()

add_arrow_lib(gandiva
              SOURCES
              ${SRC_FILES}
              OUTPUTS
              GANDIVA_LIBRARIES
              DEPENDENCIES
              arrow_dependencies
              precompiled
              EXTRA_INCLUDES
              $<TARGET_PROPERTY:LLVM::LLVM_INTERFACE,INTERFACE_INCLUDE_DIRECTORIES>
              SHARED_LINK_FLAGS
              ${GANDIVA_SHARED_LINK_FLAGS}
              SHARED_LINK_LIBS
              arrow_shared
              SHARED_PRIVATE_LINK_LIBS
              ${GANDIVA_SHARED_PRIVATE_LINK_LIBS}
              STATIC_LINK_LIBS
              ${GANDIVA_STATIC_LINK_LIBS})

foreach(LIB_TARGET ${GANDIVA_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE GANDIVA_EXPORTING)
endforeach()

if(ARROW_BUILD_STATIC AND WIN32)
  target_compile_definitions(gandiva_static PUBLIC GANDIVA_STATIC)
endif()

add_dependencies(gandiva ${GANDIVA_LIBRARIES})

# install for gandiva
include(GNUInstallDirs)

# install the header files.
arrow_install_all_headers("gandiva")

# pkg-config support
arrow_add_pkg_config("gandiva")

set(GANDIVA_STATIC_TEST_LINK_LIBS gandiva_static ${RE2_LIBRARY} ${ARROW_TEST_LINK_LIBS})

set(GANDIVA_SHARED_TEST_LINK_LIBS gandiva_shared ${RE2_LIBRARY} ${ARROW_TEST_LINK_LIBS})

function(ADD_GANDIVA_TEST REL_TEST_NAME)
  set(options USE_STATIC_LINKING)
  set(one_value_args)
  set(multi_value_args)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(NO_TESTS)
    return()
  endif()

  set(TEST_ARGUMENTS
      ENABLED
      PREFIX
      "gandiva"
      LABELS
      "gandiva-tests"
      ${ARG_UNPARSED_ARGUMENTS})

  # and uses less disk space, but in some cases we need to force static
  # linking (see rationale below).
  if(ARG_USE_STATIC_LINKING)
    add_test_case(${REL_TEST_NAME} ${TEST_ARGUMENTS} STATIC_LINK_LIBS
                  ${GANDIVA_STATIC_TEST_LINK_LIBS})
  else()
    add_test_case(${REL_TEST_NAME} ${TEST_ARGUMENTS} STATIC_LINK_LIBS
                  ${GANDIVA_SHARED_TEST_LINK_LIBS})
  endif()

  set(TARGET_NAME gandiva-${REL_TEST_NAME})

  if((TARGET ${TARGET_NAME})
     AND (${REL_TEST_NAME} MATCHES "llvm"
          OR ${REL_TEST_NAME} MATCHES "expression_registry"))
    # If the unit test has llvm in its name, include llvm.
    add_dependencies(${TARGET_NAME} LLVM::LLVM_INTERFACE)
    target_link_libraries(${TARGET_NAME} PRIVATE LLVM::LLVM_INTERFACE)
  endif()
endfunction()

add_gandiva_test(bitmap_accumulator_test)
add_gandiva_test(engine_llvm_test)
add_gandiva_test(function_signature_test)
add_gandiva_test(function_registry_test)
add_gandiva_test(llvm_types_test)
add_gandiva_test(llvm_generator_test)
add_gandiva_test(annotator_test)
add_gandiva_test(tree_expr_test)
add_gandiva_test(expr_decomposer_test)
add_gandiva_test(expression_registry_test)
add_gandiva_test(selection_vector_test)
add_gandiva_test(lru_cache_test)
add_gandiva_test(to_date_holder_test)
add_gandiva_test(simple_arena_test)
add_gandiva_test(like_holder_test)
add_gandiva_test(decimal_type_util_test)

if(ARROW_GANDIVA_JAVA)
  add_subdirectory(jni)
endif()

add_subdirectory(precompiled)
add_subdirectory(tests)
