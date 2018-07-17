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

find_package(Boost COMPONENTS system regex filesystem REQUIRED)

set(BC_FILE_PATH_CC "${CMAKE_CURRENT_BINARY_DIR}/bc_file_path.cc")
configure_file(bc_file_path.cc.in ${BC_FILE_PATH_CC})

set(SRC_FILES annotator.cc
      bitmap_accumulator.cc
      configuration.cc
      engine.cc
      expr_decomposer.cc
      expr_validator.cc
      expression.cc
      expression_registry.cc
      function_registry.cc
      function_signature.cc
      llvm_generator.cc
      llvm_types.cc
      projector.cc
      status.cc
      tree_expr_builder.cc
      ${BC_FILE_PATH_CC})

add_library(gandiva_obj_lib OBJECT ${SRC_FILES})

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
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    $<TARGET_PROPERTY:LLVM::LLVM_INTERFACE,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:ARROW::ARROW_SHARED,INTERFACE_INCLUDE_DIRECTORIES>
    $<TARGET_PROPERTY:Boost::boost,INTERFACE_INCLUDE_DIRECTORIES>
)

build_gandiva_lib("shared")

build_gandiva_lib("static")

# install for gandiva
include(GNUInstallDirs)

# install libgandiva
install(
  TARGETS gandiva_shared gandiva_static
  DESTINATION ${CMAKE_INSTALL_LIBDIR}
)

# install the header files.
install(
  DIRECTORY ${CMAKE_SOURCE_DIR}/include/gandiva
  DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

#args: label test-file src-files
add_gandiva_unit_test(bitmap_accumulator_test.cc bitmap_accumulator.cc)
add_gandiva_unit_test(dex_llvm_test.cc)
add_gandiva_unit_test(engine_llvm_test.cc engine.cc llvm_types.cc status.cc configuration.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(function_signature_test.cc function_signature.cc)
add_gandiva_unit_test(function_registry_test.cc function_registry.cc function_signature.cc)
add_gandiva_unit_test(llvm_types_test.cc llvm_types.cc)
add_gandiva_unit_test(llvm_generator_test.cc llvm_generator.cc engine.cc llvm_types.cc expr_decomposer.cc function_registry.cc annotator.cc status.cc bitmap_accumulator.cc configuration.cc  function_signature.cc ${BC_FILE_PATH_CC})
add_gandiva_unit_test(annotator_test.cc annotator.cc function_signature.cc)
add_gandiva_unit_test(tree_expr_test.cc tree_expr_builder.cc expr_decomposer.cc annotator.cc function_registry.cc function_signature.cc)
add_gandiva_unit_test(expr_decomposer_test.cc expr_decomposer.cc tree_expr_builder.cc annotator.cc function_registry.cc function_signature.cc)
add_gandiva_unit_test(status_test.cc status.cc)
add_gandiva_unit_test(expression_registry_test.cc llvm_types.cc expression_registry.cc function_signature.cc function_registry.cc)

