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
# arrow_io : Arrow IO interfaces

# HDFS thirdparty setup
if (DEFINED ENV{HADOOP_HOME})
  set(HADOOP_HOME $ENV{HADOOP_HOME})
  if (NOT EXISTS "${HADOOP_HOME}/include/hdfs.h")
    message(STATUS "Did not find hdfs.h in expected location, using vendored one")
    set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
  endif()
else()
  set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
endif()

set(HDFS_H_PATH "${HADOOP_HOME}/include/hdfs.h")
if (NOT EXISTS ${HDFS_H_PATH})
  message(FATAL_ERROR "Did not find hdfs.h at ${HDFS_H_PATH}")
endif()
message(STATUS "Found hdfs.h at: " ${HDFS_H_PATH})

include_directories(SYSTEM "${HADOOP_HOME}/include")

# arrow_io library
if (MSVC)
  set(ARROW_IO_STATIC_LINK_LIBS
    arrow_static
  )
  set(ARROW_IO_SHARED_LINK_LIBS
    arrow_shared
  )
else()
  set(ARROW_IO_STATIC_LINK_LIBS
    arrow_static
    dl
  )
  set(ARROW_IO_SHARED_LINK_LIBS
    arrow_shared
    dl
  )
endif()

if (ARROW_BOOST_USE_SHARED)
  set(ARROW_IO_SHARED_PRIVATE_LINK_LIBS
    boost_system_shared
    boost_filesystem_shared)
else()
  set(ARROW_IO_SHARED_PRIVATE_LINK_LIBS
    boost_system_static
    boost_filesystem_static)
endif()

set(ARROW_IO_STATIC_PRIVATE_LINK_LIBS
  boost_system_static
  boost_filesystem_static)

set(ARROW_IO_TEST_LINK_LIBS
  arrow_io_static)

set(ARROW_IO_SRCS
  file.cc
  hdfs.cc
  hdfs-internal.cc
  interfaces.cc
  memory.cc
)

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_IO_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

ADD_ARROW_LIB(arrow_io
  SOURCES ${ARROW_IO_SRCS}
  SHARED_LINK_FLAGS ${ARROW_IO_LINK_FLAGS}
  SHARED_LINK_LIBS ${ARROW_IO_SHARED_LINK_LIBS}
  SHARED_PRIVATE_LINK_LIBS ${ARROW_IO_SHARED_PRIVATE_LINK_LIBS}
  STATIC_LINK_LIBS ${ARROW_IO_STATIC_LINK_LIBS}
  STATIC_PRIVATE_LINK_LIBS ${ARROW_IO_STATIC_PRIVATE_LINK_LIBS}
)

ADD_ARROW_TEST(io-file-test)
ARROW_TEST_LINK_LIBRARIES(io-file-test
  ${ARROW_IO_TEST_LINK_LIBS})

ADD_ARROW_TEST(io-hdfs-test)
ARROW_TEST_LINK_LIBRARIES(io-hdfs-test
  ${ARROW_IO_TEST_LINK_LIBS})

ADD_ARROW_TEST(io-memory-test)
ARROW_TEST_LINK_LIBRARIES(io-memory-test
  ${ARROW_IO_TEST_LINK_LIBS})

# Headers: top level
install(FILES
  file.h
  hdfs.h
  interfaces.h
  memory.h
  DESTINATION include/arrow/io)

# pkg-config support
configure_file(arrow-io.pc.in
  "${CMAKE_CURRENT_BINARY_DIR}/arrow-io.pc"
  @ONLY)
install(
  FILES "${CMAKE_CURRENT_BINARY_DIR}/arrow-io.pc"
  DESTINATION "lib/pkgconfig/")
