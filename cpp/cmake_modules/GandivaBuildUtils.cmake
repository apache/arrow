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

set(GANDIVA_TEST_LINK_LIBS
  gtest_static
  gtest_main_static
  ${RE2_LIBRARY})

if (PTHREAD_LIBRARY)
  set(GANDIVA_TEST_LINK_LIBS
    ${GANDIVA_TEST_LINK_LIBS}
    ${PTHREAD_LIBRARY})
endif()

# Add a unittest executable, with its dependencies.
function(add_gandiva_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  if(${REL_TEST_NAME} MATCHES "llvm" OR
     ${REL_TEST_NAME} MATCHES "expression_registry")
    # If the unit test has llvm in its name, include llvm.
    add_dependencies(${TEST_NAME} LLVM::LLVM_INTERFACE)
    target_link_libraries(${TEST_NAME} PRIVATE LLVM::LLVM_INTERFACE)
  endif()

  # Require toolchain to be built
  add_dependencies(${TEST_NAME} arrow_dependencies)

  target_include_directories(${TEST_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/src
  )
  target_link_libraries(${TEST_NAME}
    PRIVATE arrow_shared ${GANDIVA_TEST_LINK_LIBS}
  )
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS gandiva,unittest ${TEST_NAME})
endfunction(add_gandiva_unit_test REL_TEST_NAME)

# Add a unittest executable for a precompiled file (used to generate IR)
function(add_precompiled_unit_test REL_TEST_NAME)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME} ${REL_TEST_NAME} ${ARGN})
  # Require toolchain to be built
  add_dependencies(${TEST_NAME} arrow_dependencies)
  target_include_directories(${TEST_NAME} PRIVATE ${CMAKE_SOURCE_DIR}/src)
  target_link_libraries(${TEST_NAME}
    PRIVATE arrow_shared ${GANDIVA_TEST_LINK_LIBS}
  )
  target_compile_definitions(${TEST_NAME} PRIVATE GANDIVA_UNIT_TEST=1)
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS gandiva,unittest ${TEST_NAME})
endfunction(add_precompiled_unit_test REL_TEST_NAME)

# Add an integ executable, with its dependencies.
function(add_gandiva_integ_test REL_TEST_NAME GANDIVA_LIB)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME}_${GANDIVA_LIB} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME}_${GANDIVA_LIB} PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(${TEST_NAME}_${GANDIVA_LIB} PRIVATE
    ${GANDIVA_LIB}
    ${GANDIVA_TEST_LINK_LIBS}
  )

  add_test(NAME ${TEST_NAME}_${GANDIVA_LIB} COMMAND ${TEST_NAME}_${GANDIVA_LIB})
  set_property(TEST ${TEST_NAME}_${GANDIVA_LIB} PROPERTY LABELS gandiva,integ ${TEST_NAME}_${GANDIVA_LIB})
endfunction(add_gandiva_integ_test REL_TEST_NAME)

function(prevent_in_source_builds)
 file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
 if(EXISTS "${LOC_PATH}")
   message(FATAL_ERROR "Gandiva does not support in-source builds")
 endif()
endfunction(prevent_in_source_builds)
