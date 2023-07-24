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
# specific language governing permissions and limitationsn
# under the License.

# add the client subdirectory
add_subdirectory(client)

# define the targets to build
add_custom_target(arrow_skyhook)
add_custom_target(cls_skyhook)

# define the dependencies
find_package(librados REQUIRED)
set(ARROW_SKYHOOK_LINK_STATIC arrow_dataset_static librados::rados)
set(ARROW_SKYHOOK_LINK_SHARED arrow_dataset_shared librados::rados)

# define the client and cls sources
set(ARROW_SKYHOOK_CLIENT_SOURCES client/file_skyhook.cc protocol/rados_protocol.cc
                                 protocol/skyhook_protocol.cc)
set(ARROW_SKYHOOK_CLS_SOURCES cls/cls_skyhook.cc protocol/rados_protocol.cc
                              protocol/skyhook_protocol.cc)

# define the client library
add_arrow_lib(arrow_skyhook
              PKG_CONFIG_NAME
              skyhook
              SOURCES
              ${ARROW_SKYHOOK_CLIENT_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_CLIENT_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_SKYHOOK_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_SKYHOOK_LINK_STATIC})

#  define the cls library
add_arrow_lib(cls_skyhook
              SOURCES
              ${ARROW_SKYHOOK_CLS_SOURCES}
              OUTPUTS
              ARROW_SKYHOOK_CLS_LIBRARIES
              SHARED_LINK_LIBS
              ${ARROW_SKYHOOK_LINK_SHARED}
              STATIC_LINK_LIBS
              ${ARROW_SKYHOOK_LINK_STATIC})

# finish building the project
add_dependencies(arrow_skyhook ${ARROW_SKYHOOK_CLIENT_LIBRARIES})
add_dependencies(cls_skyhook ${ARROW_SKYHOOK_CLS_LIBRARIES})

# define the test builds
if(ARROW_TEST_LINKAGE STREQUAL "static")
  set(ARROW_SKYHOOK_TEST_LINK_LIBS arrow_skyhook_static arrow_dataset_static
                                   ${ARROW_TEST_STATIC_LINK_LIBS})
else()
  set(ARROW_SKYHOOK_TEST_LINK_LIBS arrow_skyhook_shared arrow_dataset_shared
                                   ${ARROW_TEST_SHARED_LINK_LIBS})
endif()

# build the cls and protocol tests
add_arrow_test(cls_test
               SOURCES
               cls/cls_skyhook_test.cc
               EXTRA_LINK_LIBS
               ${ARROW_SKYHOOK_TEST_LINK_LIBS}
               PREFIX
               "skyhook")

add_arrow_test(protocol_test
               SOURCES
               protocol/skyhook_protocol_test.cc
               EXTRA_LINK_LIBS
               ${ARROW_SKYHOOK_TEST_LINK_LIBS}
               PREFIX
               "skyhook")
