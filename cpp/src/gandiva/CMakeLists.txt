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

if(ARROW_DEPENDENCY_SOURCE STREQUAL "CONDA")
  set(LLVM_ROOT $ENV{CONDA_PREFIX})
endif()
find_package(LLVM)

# Set the path where the bitcode file generated, see precompiled/CMakeLists.txt
set(GANDIVA_PRECOMPILED_BC_PATH "${CMAKE_CURRENT_BINARY_DIR}/irhelpers.bc")
set(GANDIVA_PRECOMPILED_CC_PATH "${CMAKE_CURRENT_BINARY_DIR}/precompiled-bitcode.cc")
set(GANDIVA_PRECOMPILED_CC_IN_PATH
    "${CMAKE_CURRENT_SOURCE_DIR}/precompiled-bitcode.cc.in")

# add_arrow_lib will look for this not yet existing file, so flag as generated
set_source_files_properties(${GANDIVA_PRECOMPILED_CC_PATH} PROPERTIES GENERATED TRUE)

set(SRC_FILES
    annotator.cc
    bitmap-accumulator.cc
    cast-time.cc
    configuration.cc
    context-helper.cc
    decimal-ir.cc
    decimal-type-util.cc
    decimal-xlarge.cc
    engine.cc
    date-utils.cc
    expr-decomposer.cc
    expr-validator.cc
    expression.cc
    expression-registry.cc
    exported-funcs-registry.cc
    filter.cc
    function-ir-builder.cc
    function-registry.cc
    function-registry-arithmetic.cc
    function-registry-datetime.cc
    function-registry-hash.cc
    function-registry-math-ops.cc
    function-registry-string.cc
    function-registry-timestamp-arithmetic.cc
    function-signature.cc
    gdv-function-stubs.cc
    llvm-generator.cc
    llvm-types.cc
    like-holder.cc
    literal-holder.cc
    projector.cc
    regex-util.cc
    selection-vector.cc
    tree-expr-builder.cc
    to-date-holder.cc
    ${GANDIVA_PRECOMPILED_CC_PATH})

set(GANDIVA_SHARED_PRIVATE_LINK_LIBS arrow_shared LLVM::LLVM_INTERFACE RE2::re2)

set(GANDIVA_STATIC_LINK_LIBS arrow_static LLVM::LLVM_INTERFACE RE2::re2)

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

set(GANDIVA_STATIC_TEST_LINK_LIBS gandiva_static RE2::re2 ${ARROW_TEST_LINK_LIBS})

set(GANDIVA_SHARED_TEST_LINK_LIBS gandiva_shared RE2::re2 ${ARROW_TEST_LINK_LIBS})

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
    add_test_case(${REL_TEST_NAME}
                  ${TEST_ARGUMENTS}
                  STATIC_LINK_LIBS
                  ${GANDIVA_STATIC_TEST_LINK_LIBS}
                  ${ARG_UNPARSED_ARGUMENTS})
  else()
    add_test_case(${REL_TEST_NAME}
                  ${TEST_ARGUMENTS}
                  STATIC_LINK_LIBS
                  ${GANDIVA_SHARED_TEST_LINK_LIBS}
                  ${ARG_UNPARSED_ARGUMENTS})
  endif()

  set(TARGET_NAME gandiva-${REL_TEST_NAME})

  # If the unit test has llvm in its name, include llvm.
  add_dependencies(${TARGET_NAME} LLVM::LLVM_INTERFACE)
  target_link_libraries(${TARGET_NAME} PRIVATE LLVM::LLVM_INTERFACE)
endfunction()

add_gandiva_test(internals-test
                 SOURCES
                 bitmap-accumulator-test.cc
                 engine-llvm-test.cc
                 function-signature-test.cc
                 function-registry-test.cc
                 llvm-types-test.cc
                 llvm-generator-test.cc
                 annotator-test.cc
                 tree-expr-test.cc
                 expr-decomposer-test.cc
                 expression-registry-test.cc
                 selection-vector-test.cc
                 lru-cache-test.cc
                 to-date-holder-test.cc
                 simple-arena-test.cc
                 like-holder-test.cc
                 decimal-type-util-test.cc)

if(ARROW_GANDIVA_JAVA)
  add_subdirectory(jni)
endif()

add_subdirectory(precompiled)
add_subdirectory(tests)
