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

add_custom_target(arrow_substrait)

arrow_install_all_headers("arrow/engine")

set(ARROW_SUBSTRAIT_SRCS
    substrait/expression_internal.cc
    substrait/extension_set.cc
    substrait/extension_types.cc
    substrait/options.cc
    substrait/plan_internal.cc
    substrait/relation_internal.cc
    substrait/serde.cc
    substrait/test_plan_builder.cc
    substrait/type_internal.cc
    substrait/util_internal.cc
    substrait/util.cc)

add_arrow_lib(arrow_substrait
              CMAKE_PACKAGE_NAME
              ArrowSubstrait
              PKG_CONFIG_NAME
              arrow-substrait
              OUTPUTS
              ARROW_SUBSTRAIT_LIBRARIES
              SOURCES
              ${ARROW_SUBSTRAIT_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/engine/pch.h>"
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_dataset_shared
              substrait
              SHARED_INSTALL_INTERFACE_LIBS
              ArrowDataset::arrow_dataset_shared
              STATIC_LINK_LIBS
              arrow_dataset_static
              substrait
              STATIC_INSTALL_INTERFACE_LIBS
              ArrowDataset::arrow_dataset_static
              PRIVATE_INCLUDES
              ${SUBSTRAIT_INCLUDES})

foreach(LIB_TARGET ${ARROW_SUBSTRAIT_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_ENGINE_EXPORTING)
endforeach()

set(ARROW_SUBSTRAIT_TEST_LINK_LIBS ${ARROW_SUBSTRAIT_LINK_lIBS} ${ARROW_TEST_LINK_LIBS})
if(ARROW_TEST_LINKAGE STREQUAL "static")
  list(APPEND ARROW_SUBSTRAIT_TEST_LINK_LIBS arrow_substrait_static)
else()
  list(APPEND ARROW_SUBSTRAIT_TEST_LINK_LIBS arrow_substrait_shared)
endif()

add_arrow_test(substrait_test
               SOURCES
               substrait/ext_test.cc
               substrait/function_test.cc
               substrait/serde_test.cc
               substrait/protobuf_test_util.cc
               EXTRA_LINK_LIBS
               ${ARROW_SUBSTRAIT_TEST_LINK_LIBS}
               PREFIX
               "arrow-substrait"
               LABELS
               "arrow_substrait")

add_subdirectory(substrait)
