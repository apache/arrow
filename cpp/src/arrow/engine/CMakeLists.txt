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

add_custom_target(arrow_engine)

arrow_install_all_headers("arrow/engine")

set(ARROW_ENGINE_SRCS
    substrait/expression_internal.cc
    substrait/extension_set.cc
    substrait/extension_types.cc
    substrait/serde.cc
    substrait/plan_internal.cc
    substrait/relation_internal.cc
    substrait/type_internal.cc)

add_arrow_lib(arrow_engine
              CMAKE_PACKAGE_NAME
              ArrowEngine
              PKG_CONFIG_NAME
              arrow-engine
              OUTPUTS
              ARROW_ENGINE_LIBRARIES
              SOURCES
              ${ARROW_ENGINE_SRCS}
              PRECOMPILED_HEADERS
              "$<$<COMPILE_LANGUAGE:CXX>:arrow/engine/pch.h>"
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_shared
              arrow_dataset_shared
              substrait
              STATIC_LINK_LIBS
              arrow_static
              arrow_dataset_static
              substrait
              PRIVATE_INCLUDES
              ${SUBSTRAIT_INCLUDES})

foreach(LIB_TARGET ${ARROW_ENGINE_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_ENGINE_EXPORTING)
endforeach()

set(ARROW_ENGINE_TEST_LINK_LIBS ${ARROW_ENGINE_LINK_lIBS} ${ARROW_TEST_LINK_LIBS})
if(ARROW_TEST_LINKAGE STREQUAL "static")
  list(APPEND ARROW_ENGINE_TEST_LINK_LIBS arrow_engine_static)
else()
  list(APPEND ARROW_ENGINE_TEST_LINK_LIBS arrow_engine_shared)
endif()

add_arrow_test(substrait_test
               SOURCES
               substrait/serde_test.cc
               EXTRA_LINK_LIBS
               ${ARROW_ENGINE_TEST_LINK_LIBS}
               PREFIX
               "arrow-engine"
               LABELS
               "arrow_engine")

add_subdirectory(substrait)
