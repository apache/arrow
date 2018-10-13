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

# Build the gandiva library
function(build_gandiva_lib TYPE ARROW)
  string(TOUPPER ${TYPE} TYPE_UPPER_CASE)
  add_library(gandiva_${TYPE} ${TYPE_UPPER_CASE} $<TARGET_OBJECTS:gandiva_obj_lib>)
  add_dependencies(gandiva_${TYPE} arrow_dependencies)

  target_include_directories(gandiva_${TYPE}
    PUBLIC
      $<INSTALL_INTERFACE:include>
      $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    PRIVATE
      ${CMAKE_SOURCE_DIR}/src
  )

  # ARROW is a public dependency i.e users of gandiva also will have a dependency on arrow.
  target_link_libraries(gandiva_${TYPE}
    PUBLIC
      ${ARROW}
    PRIVATE
      Boost::boost
      Boost::regex
      Boost::system
      Boost::filesystem
      LLVM::LLVM_INTERFACE
      re2)

  if (${TYPE} MATCHES "static" AND NOT APPLE)
    target_link_libraries(gandiva_${TYPE}
      LINK_PRIVATE
        -static-libstdc++ -static-libgcc)
  endif()

  # Set version for the library.
  set(GANDIVA_VERSION_MAJOR 0)
  set(GANDIVA_VERSION_MINOR 1)
  set(GANDIVA_VERSION_PATCH 0)
  set(GANDIVA_VERSION ${GANDIVA_VERSION_MAJOR}.${GANDIVA_VERSION_MINOR}.${GANDIVA_VERSION_PATCH})

  set_target_properties(gandiva_${TYPE} PROPERTIES
    VERSION ${GANDIVA_VERSION}
    SOVERSION ${GANDIVA_VERSION_MAJOR}
    OUTPUT_NAME gandiva
  )
endfunction(build_gandiva_lib TYPE)

set(GANDIVA_TEST_LINK_LIBS
  gtest
  gtest_main
  re2)

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
    PRIVATE arrow_shared ${GANDIVA_TEST_LINK_LIBS} Boost::boost
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
  target_link_libraries(${TEST_NAME} PRIVATE ${GANDIVA_TEST_LINK_LIBS})
  target_compile_definitions(${TEST_NAME} PRIVATE GANDIVA_UNIT_TEST=1)
  target_compile_definitions(${TEST_NAME} PRIVATE -DGDV_HELPERS)
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  set_property(TEST ${TEST_NAME} PROPERTY LABELS gandiva,unittest ${TEST_NAME})
endfunction(add_precompiled_unit_test REL_TEST_NAME)

# Add an integ executable, with its dependencies.
function(add_gandiva_integ_test REL_TEST_NAME GANDIVA_LIB)
  get_filename_component(TEST_NAME ${REL_TEST_NAME} NAME_WE)

  add_executable(${TEST_NAME}_${GANDIVA_LIB} ${REL_TEST_NAME} ${ARGN})
  target_include_directories(${TEST_NAME}_${GANDIVA_LIB} PRIVATE ${CMAKE_SOURCE_DIR})
  target_link_libraries(${TEST_NAME}_${GANDIVA_LIB} PRIVATE ${GANDIVA_LIB} ${GANDIVA_TEST_LINK_LIBS})

  add_test(NAME ${TEST_NAME}_${GANDIVA_LIB} COMMAND ${TEST_NAME}_${GANDIVA_LIB})
  set_property(TEST ${TEST_NAME}_${GANDIVA_LIB} PROPERTY LABELS gandiva,integ ${TEST_NAME}_${GANDIVA_LIB})
endfunction(add_gandiva_integ_test REL_TEST_NAME)

function(prevent_in_source_builds)
 file(TO_CMAKE_PATH "${PROJECT_BINARY_DIR}/CMakeLists.txt" LOC_PATH)
 if(EXISTS "${LOC_PATH}")
   message(FATAL_ERROR "Gandiva does not support in-source builds")
 endif()
endfunction(prevent_in_source_builds)
