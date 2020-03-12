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

set(ARROW_ENGINE_SRCS catalog.cc expression.cc logical_plan.cc)

set(ARROW_ENGINE_LINK_STATIC arrow_static arrow_dataset_static)
set(ARROW_ENGINE_LINK_SHARED arrow_shared arrow_dataset_shared)

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
              PRIVATE_INCLUDES
              ${ARROW_ENGINE_PRIVATE_INCLUDES}
              SHARED_LINK_LIBS
              ${ARROW_ENGINE_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_ENGINE_LINK_STATIC})

if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_ENGINE_TEST_LINK_LIBS arrow_engine_static ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_ENGINE_TEST_LINK_LIBS arrow_engine_shared ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

foreach(LIB_TARGET ${ARROW_ENGINE_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_DS_EXPORTING)
endforeach()

# Adding unit tests part of the "engine" portion of the test suite
function(ADD_ARROW_ENGINE_TEST REL_TEST_NAME)
  set(options)
  set(one_value_args PREFIX)
  set(multi_value_args LABELS)
  cmake_parse_arguments(ARG
                        "${options}"
                        "${one_value_args}"
                        "${multi_value_args}"
                        ${ARGN})

  if(ARG_PREFIX)
    set(PREFIX ${ARG_PREFIX})
  else()
    set(PREFIX "arrow-engine")
  endif()

  if(ARG_LABELS)
    set(LABELS ${ARG_LABELS})
  else()
    set(LABELS "arrow_engine")
  endif()

  add_arrow_test(${REL_TEST_NAME}
                 EXTRA_LINK_LIBS
                 ${ARROW_ENGINE_TEST_LINK_LIBS}
                 PREFIX
                 ${PREFIX}
                 LABELS
                 ${LABELS}
                 ${ARG_UNPARSED_ARGUMENTS})
endfunction()

#
# Unit tests
#

add_arrow_engine_test(catalog_test PREFIX arrow-engine)
add_arrow_engine_test(expression_test PREFIX arrow-engine)
add_arrow_engine_test(logical_plan_test PREFIX arrow-engine)
