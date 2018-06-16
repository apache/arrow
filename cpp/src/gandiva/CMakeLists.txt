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

# Find arrow
find_package(ARROW)

find_package(Boost REQUIRED)

set(BC_FILE_PATH_CC "${CMAKE_CURRENT_BINARY_DIR}/bc_file_path.cc")
configure_file(bc_file_path.cc.in ${BC_FILE_PATH_CC})

add_library(gandiva SHARED
  annotator.cc
  engine.cc
  expr_decomposer.cc
  function_registry.cc
  llvm_generator.cc
  llvm_types.cc
  projector.cc
  status.cc
  tree_expr_builder.cc
  expr_validator.cc
  ${BC_FILE_PATH_CC})

# For users of gandiva library (including integ tests), include-dir is :
#   /usr/**/include dir after install,
#   cpp/include during build
# For building gandiva library itself, include-dir (in addition to above) is :
#   cpp/src
target_include_directories(gandiva
  PUBLIC
    $<INSTALL_INTERFACE:include>
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
)

# ARROW is a public dependency i.e users of gandiva also will have a dependency on arrow.
target_link_libraries(gandiva
  PUBLIC
    ARROW::ARROW
  PRIVATE
    Boost::boost)

# LLVM is a private dependency i.e users of gandiva will not need to include llvm headers
# or link with llvm libraries.
target_link_llvm(gandiva PRIVATE)

# Set version for the library.
set(GANDIVA_VERSION_MAJOR 0)
set(GANDIVA_VERSION_MINOR 1)
set(GANDIVA_VERSION_PATCH 0)
set(GANDIVA_VERSION ${GANDIVA_VERSION_MAJOR}.${GANDIVA_VERSION_MINOR}.${GANDIVA_VERSION_PATCH})

set_target_properties(gandiva PROPERTIES
  VERSION ${GANDIVA_VERSION}
  SOVERSION ${GANDIVA_VERSION_MAJOR}
)

# install for gandiva
include(GNUInstallDirs)

# install libgandiva
install(
  TARGETS gandiva
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# install the header files.
install(
  DIRECTORY ${CMAKE_SOURCE_DIR}/include/gandiva
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

#args: label test-file src-files
add_gandiva_unit_test(dex_llvm_test.cc)
add_gandiva_unit_test(engine_llvm_test.cc engine.cc llvm_types.cc status.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(function_signature_test.cc)
add_gandiva_unit_test(function_registry_test.cc function_registry.cc)
add_gandiva_unit_test(llvm_types_test.cc llvm_types.cc)
add_gandiva_unit_test(llvm_generator_test.cc llvm_generator.cc engine.cc llvm_types.cc expr_decomposer.cc function_registry.cc annotator.cc status.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(annotator_test.cc annotator.cc)
add_gandiva_unit_test(tree_expr_test.cc tree_expr_builder.cc expr_decomposer.cc annotator.cc function_registry.cc)
add_gandiva_unit_test(expr_decomposer_test.cc expr_decomposer.cc tree_expr_builder.cc annotator.cc function_registry.cc)
add_gandiva_unit_test(status_test.cc status.cc)
