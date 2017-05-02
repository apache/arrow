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
# parquet_arrow : Arrow <-> Parquet adapter

set(PARQUET_ARROW_SRCS
  reader.cc
  schema.cc
  writer.cc
)

# Set dependencies so ExternalProjects are built beforehand
set(PARQUET_ARROW_DEPENDENCIES ${ARROW_LINK_LIBS} parquet_static)
SET(PARQUET_ARROW_SHARED_LINK_LIBS ${ARROW_LINK_LIBS} parquet_shared)
SET(PARQUET_ARROW_STATIC_LINK_LIBS ${ARROW_LINK_LIBS} parquet_static)

ADD_LIB(parquet_arrow
        SOURCES ${PARQUET_ARROW_SRCS}
        LIB_BUILD_SHARED ${PARQUET_BUILD_SHARED}
        LIB_BUILD_STATIC ${PARQUET_BUILD_STATIC}
        DEPENDENCIES ${PARQUET_ARROW_DEPENDENCIES}
        SHARED_LINK_FLAGS ${SHARED_LINK_FLAGS}
        SHARED_LINK_LIBS ${PARQUET_ARROW_SHARED_LINK_LIBS}
        STATIC_LINK_LIBS ${PARQUET_ARROW_STATIC_LINK_LIBS}
        ABI_VERSION ${PARQUET_ABI_VERSION}
        SO_VERSION ${PARQUET_SO_VERSION}
        LIB_RPATH_ORIGIN ${PARQUET_RPATH_ORIGIN}
)

ADD_PARQUET_TEST(arrow-schema-test)
ADD_PARQUET_TEST(arrow-reader-writer-test)

if (PARQUET_BUILD_STATIC)
  ADD_PARQUET_LINK_LIBRARIES(arrow-schema-test parquet_arrow_static)
  ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-test parquet_arrow_static)
else()
  ADD_PARQUET_LINK_LIBRARIES(arrow-schema-test parquet_arrow_shared)
  ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-test parquet_arrow_shared)
endif()

if(PARQUET_BUILD_BENCHMARKS)
  ADD_PARQUET_BENCHMARK(arrow-reader-writer-benchmark)
  if (PARQUET_BUILD_STATIC)
    ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-benchmark parquet_arrow_static)
  else()
    ADD_PARQUET_LINK_LIBRARIES(arrow-reader-writer-benchmark parquet_arrow_shared)
  endif()
endif()

# Headers: top level
install(FILES
  reader.h
  schema.h
  writer.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/parquet/arrow")

# pkg-config support
configure_file(parquet-arrow.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/parquet-arrow.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/parquet-arrow.pc"
  DESTINATION "${CMAKE_INSTALL_LIBDIR}/pkgconfig/")
