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

if(NO_TESTS)
  return()
endif()
#
## copy the testing data into the build directory
add_custom_target(extension-tests-data
                  COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}
                          ${CMAKE_CURRENT_BINARY_DIR})

set(TEST_PRECOMPILED_SOURCES multiply_by_two.cc)
set(TEST_PRECOMPILED_BC_FILES)
foreach(SOURCE ${TEST_PRECOMPILED_SOURCES})
  gandiva_add_bitcode(${SOURCE})
  get_filename_component(SOURCE_BASE ${SOURCE} NAME_WE)
  list(APPEND TEST_PRECOMPILED_BC_FILES ${CMAKE_CURRENT_BINARY_DIR}/${SOURCE_BASE}.bc)
endforeach()
add_custom_target(extension-tests ALL DEPENDS extension-tests-data
                                              ${TEST_PRECOMPILED_BC_FILES})
#
## set the GANDIVA_EXTENSION_TEST_DIR macro so that the tests can pass regardless where they are run from
## corresponding extension test data files and bitcode will be copied/generated
set(TARGETS_DEPENDING_ON_TEST_BITCODE_FILES gandiva-internals-test gandiva-projector-test
                                            gandiva-projector-test-static)
foreach(TARGET ${TARGETS_DEPENDING_ON_TEST_BITCODE_FILES})
  if(TARGET ${TARGET})
    add_dependencies(${TARGET} extension-tests)
    target_compile_definitions(${TARGET}
                               PRIVATE -DGANDIVA_EXTENSION_TEST_DIR="${CMAKE_CURRENT_BINARY_DIR}"
    )
  endif()
endforeach()

add_dependencies(gandiva-tests extension-tests)
