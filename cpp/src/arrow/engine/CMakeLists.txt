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

set(ARROW_ENGINE_LINK_LIBS ${ARROW_PROTOBUF_LIBPROTOBUF})

#if(WIN32)
#  list(APPEND ARROW_ENGINE_LINK_LIBS ws2_32.lib)
#endif()

set(ARROW_ENGINE_SRCS
    substrait/expression_internal.cc
    substrait/extension_set.cc
    substrait/extension_types.cc
    substrait/serde.cc
    substrait/plan_internal.cc
    substrait/relation_internal.cc
    substrait/type_internal.cc)

set(SUBSTRAIT_LOCAL_DIR "${CMAKE_CURRENT_BINARY_DIR}/substrait_clone")
set(SUBSTRAIT_GEN_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(SUBSTRAIT_PROTOS
    capabilities
    expression
    extensions/extensions
    function
    parameterized_types
    plan
    relations
    type
    type_expressions)

externalproject_add(substrait_ep
                    GIT_REPOSITORY "${ARROW_SUBSTRAIT_REPO}"
                    GIT_TAG "${ARROW_SUBSTRAIT_TAG}"
                    SOURCE_DIR "${SUBSTRAIT_LOCAL_DIR}"
                    CONFIGURE_COMMAND ""
                    BUILD_COMMAND ""
                    INSTALL_COMMAND "")

set(SUBSTRAIT_SUPPRESSED_WARNINGS)
if(MSVC)
  # Protobuf generated files trigger some spurious warnings on MSVC.

  # Implicit conversion from uint64_t to uint32_t:
  list(APPEND SUBSTRAIT_SUPPRESSED_WARNINGS "/wd4244")

  # Missing dll-interface:
  list(APPEND SUBSTRAIT_SUPPRESSED_WARNINGS "/wd4251")
endif()

set(SUBSTRAIT_PROTO_GEN_ALL)
foreach(SUBSTRAIT_PROTO ${SUBSTRAIT_PROTOS})
  set(SUBSTRAIT_PROTO_GEN "${SUBSTRAIT_GEN_DIR}/substrait/${SUBSTRAIT_PROTO}.pb")

  foreach(EXT h cc)
    set_source_files_properties("${SUBSTRAIT_PROTO_GEN}.${EXT}"
                                PROPERTIES COMPILE_OPTIONS
                                           "${SUBSTRAIT_SUPPRESSED_WARNINGS}"
                                           GENERATED TRUE
                                           SKIP_UNITY_BUILD_INCLUSION TRUE)
    add_custom_command(OUTPUT "${SUBSTRAIT_PROTO_GEN}.${EXT}"
                       COMMAND ${ARROW_PROTOBUF_PROTOC} "-I${SUBSTRAIT_LOCAL_DIR}/proto"
                               "--cpp_out=${SUBSTRAIT_GEN_DIR}"
                               "${SUBSTRAIT_LOCAL_DIR}/proto/substrait/${SUBSTRAIT_PROTO}.proto"
                       DEPENDS ${PROTO_DEPENDS} substrait_ep)
    list(APPEND SUBSTRAIT_PROTO_GEN_ALL "${SUBSTRAIT_PROTO_GEN}.${EXT}")
  endforeach()

  list(APPEND ARROW_ENGINE_SRCS "${SUBSTRAIT_PROTO_GEN}.cc")
endforeach()

add_custom_target(substrait_gen ALL DEPENDS ${SUBSTRAIT_PROTO_GEN_ALL})

find_package(Git)
add_custom_target(substrait_gen_verify
                  COMMENT "Verifying that generated substrait accessors are consistent with \
                           ARROW_SUBSTRAIT_REPO_AND_TAG='${ARROW_SUBSTRAIT_REPO_AND_TAG}'"
                  COMMAND ${GIT_EXECUTABLE} diff --exit-code ${SUBSTRAIT_GEN_DIR}
                  DEPENDS substrait_gen_clear
                  DEPENDS substrait_gen)

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
              ${ARROW_ENGINE_LINK_LIBS}
              STATIC_LINK_LIBS
              arrow_static
              arrow_dataset_static
              ${ARROW_ENGINE_LINK_LIBS}
              PRIVATE_INCLUDES
              ${SUBSTRAIT_GEN_DIR})

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
