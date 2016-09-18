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

set(ARROW_IO_LINK_LIBS
  arrow_shared
  dl
)

if (ARROW_BOOST_USE_SHARED)
  set(ARROW_IO_PRIVATE_LINK_LIBS
    boost_system_shared
    boost_filesystem_shared)
else()
  set(ARROW_IO_PRIVATE_LINK_LIBS
    boost_system_static
    boost_filesystem_static)
endif()

set(ARROW_IO_TEST_LINK_LIBS
  arrow_io
  ${ARROW_IO_PRIVATE_LINK_LIBS})

set(ARROW_IO_SRCS
  memory.cc
)

if(ARROW_HDFS)
  if(NOT THIRDPARTY_DIR)
    message(FATAL_ERROR "THIRDPARTY_DIR not set")
  endif()

  if (DEFINED ENV{HADOOP_HOME})
    set(HADOOP_HOME $ENV{HADOOP_HOME})
  else()
    set(HADOOP_HOME "${THIRDPARTY_DIR}/hadoop")
  endif()

  set(HDFS_H_PATH "${HADOOP_HOME}/include/hdfs.h")
  if (NOT EXISTS ${HDFS_H_PATH})
    message(FATAL_ERROR "Did not find hdfs.h at ${HDFS_H_PATH}")
  endif()
  message(STATUS "Found hdfs.h at: " ${HDFS_H_PATH})
  message(STATUS "Building libhdfs shim component")

  include_directories(SYSTEM "${HADOOP_HOME}/include")

  set(ARROW_HDFS_SRCS
    hdfs.cc
    libhdfs_shim.cc)

  set_property(SOURCE ${ARROW_HDFS_SRCS}
    APPEND_STRING PROPERTY
    COMPILE_FLAGS "-DHAS_HADOOP")

  set(ARROW_IO_SRCS
    ${ARROW_HDFS_SRCS}
    ${ARROW_IO_SRCS})

  ADD_ARROW_TEST(io-hdfs-test)
  ARROW_TEST_LINK_LIBRARIES(io-hdfs-test
    ${ARROW_IO_TEST_LINK_LIBS})
endif()

add_library(arrow_io SHARED
  ${ARROW_IO_SRCS}
)
target_link_libraries(arrow_io
  LINK_PUBLIC ${ARROW_IO_LINK_LIBS}
  LINK_PRIVATE ${ARROW_IO_PRIVATE_LINK_LIBS})

if(NOT APPLE)
  # Localize thirdparty symbols using a linker version script. This hides them
  # from the client application. The OS X linker does not support the
  # version-script option.
  set(ARROW_IO_LINK_FLAGS "-Wl,--version-script=${CMAKE_CURRENT_SOURCE_DIR}/symbols.map")
endif()

SET_TARGET_PROPERTIES(arrow_io PROPERTIES
  LINKER_LANGUAGE CXX
  LINK_FLAGS "${ARROW_IO_LINK_FLAGS}")

if (APPLE)
  set_target_properties(arrow_io
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH ON
    INSTALL_NAME_DIR "@rpath")
endif()

ADD_ARROW_TEST(io-memory-test)
ARROW_TEST_LINK_LIBRARIES(io-memory-test
  ${ARROW_IO_TEST_LINK_LIBS})

# Headers: top level
install(FILES
  hdfs.h
  interfaces.h
  memory.h
  DESTINATION include/arrow/io)

install(TARGETS arrow_io
  LIBRARY DESTINATION lib
  ARCHIVE DESTINATION lib)
