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

# ----------------------------------------------------------------------
# arrow_jemalloc : Arrow jemalloc-based allocator

include_directories(SYSTEM "{JEMALLOC_INCLUDE_DIR}")

# In the case that jemalloc is only available as a shared library also use it to
# link it in the static requirements. In contrast to other libraries we try in
# most cases to use the system provided version of jemalloc to better align with
# other potential users of jemalloc.
if (JEMALLOC_STATIC_LIB)
  set(ARROW_JEMALLOC_STATIC_LINK_LIBS
    arrow_static
    jemalloc_static
  )
else()
  set(ARROW_JEMALLOC_STATIC_LINK_LIBS
    arrow_static
    jemalloc_shared
  )
endif()

if (NOT APPLE)
  set(ARROW_JEMALLOC_STATIC_LINK_LIBS ${ARROW_JEMALLOC_STATIC_LINK_LIBS} pthread)
endif()

set(ARROW_JEMALLOC_SHARED_LINK_LIBS
  arrow_shared
  jemalloc_shared
)

if (ARROW_BUILD_STATIC)
  set(ARROW_JEMALLOC_TEST_LINK_LIBS
    ${ARROW_JEMALLOC_STATIC_LINK_LIBS}
    arrow_jemalloc_static)
else()
  set(ARROW_JEMALLOC_TEST_LINK_LIBS
    arrow_jemalloc_shared)
endif()

set(ARROW_JEMALLOC_SRCS
  memory_pool.cc
)

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_JEMALLOC_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

ADD_ARROW_LIB(arrow_jemalloc
  SOURCES ${ARROW_JEMALLOC_SRCS}
  SHARED_LINK_FLAGS ${ARROW_JEMALLOC_LINK_FLAGS}
  SHARED_LINK_LIBS ${ARROW_JEMALLOC_SHARED_LINK_LIBS}
  SHARED_PRIVATE_LINK_LIBS ${ARROW_JEMALLOC_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_JEMALLOC_STATIC_LINK_LIBS}
  STATIC_PRIVATE_LINK_LIBS ${ARROW_JEMALLOC_STATIC_PRIVATE_LINK_LIBS}
)

ADD_ARROW_TEST(jemalloc-memory_pool-test)
ARROW_TEST_LINK_LIBRARIES(jemalloc-memory_pool-test
  ${ARROW_JEMALLOC_TEST_LINK_LIBS})

ADD_ARROW_BENCHMARK(jemalloc-builder-benchmark)
ARROW_BENCHMARK_LINK_LIBRARIES(jemalloc-builder-benchmark
  ${ARROW_JEMALLOC_TEST_LINK_LIBS})

# Headers: top level
install(FILES
  memory_pool.h
  DESTINATION include/arrow/jemalloc)

# pkg-config support
configure_file(arrow-jemalloc.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-jemalloc.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-jemalloc.pc"
  DESTINATION "lib/pkgconfig/")
