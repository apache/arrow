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

# Headers: top level
install(FILES
  api.h
  columnar-row-set.h
  operation.h
  service.h
  session.h
  types.h
  util.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/dbi/hiveserver2")

set(ARROW_HIVESERVER2_SRCS
  columnar-row-set.cc
  service.cc
  session.cc
  operation.cc
  sample-usage.cc
  thrift-internal.cc
  types.cc
  util.cc
)

add_subdirectory(thrift)

set(HIVESERVER2_THRIFT_SRC
  ErrorCodes_constants.cpp
  ErrorCodes_types.cpp
  ImpalaService.cpp
  ImpalaService_constants.cpp
  ImpalaService_types.cpp
  ImpalaHiveServer2Service.cpp
  beeswax_constants.cpp
  beeswax_types.cpp
  BeeswaxService.cpp
  TCLIService.cpp
  TCLIService_constants.cpp
  TCLIService_types.cpp
  ExecStats_constants.cpp
  ExecStats_types.cpp
  hive_metastore_constants.cpp
  hive_metastore_types.cpp
  Status_constants.cpp
  Status_types.cpp
  Types_constants.cpp
  Types_types.cpp
)

set_source_files_properties(${HIVESERVER2_THRIFT_SRC} PROPERTIES
  COMPILE_FLAGS "-Wno-unused-variable -Wno-shadow-field"
  GENERATED TRUE)

# keep everything in one library, the object files reference
# each other
add_library(arrow_hiveserver2_thrift STATIC ${HIVESERVER2_THRIFT_SRC})

# Setting these files as code-generated lets make clean and incremental builds work
# correctly

# TODO(wesm): Something is broken with the dependency chain with
# ImpalaService.cpp and others. Couldn't figure out what is different between
# this setup and Impala.

add_dependencies(arrow_hiveserver2_thrift hs2-thrift-cpp)

set_target_properties(arrow_hiveserver2_thrift
  PROPERTIES
  LIBRARY_OUTPUT_DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}")

ADD_ARROW_LIB(arrow_hiveserver2
  SOURCES ${ARROW_HIVESERVER2_SRCS}
  DEPENDENCIES arrow_hiveserver2_thrift
  SHARED_LINK_FLAGS ""
  SHARED_LINK_LIBS ${ARROW_PYTHON_SHARED_LINK_LIBS}
)

set_property(SOURCE ${ARROW_HIVESERVER2_SRCS}
  APPEND_STRING PROPERTY COMPILE_FLAGS
  " -Wno-shadow-field")

set(ARROW_HIVESERVER2_TEST_LINK_LIBS
  ${ARROW_TEST_LINK_LIBS}
  arrow_hiveserver2_static
  arrow_hiveserver2_thrift
  thriftstatic)

ADD_ARROW_TEST(hiveserver2-test
  STATIC_LINK_LIBS "${ARROW_HIVESERVER2_TEST_LINK_LIBS}"
  LABELS "hiveserver2"
)

set_property(TARGET hiveserver2-test
  APPEND_STRING PROPERTY COMPILE_FLAGS
  " -Wno-shadow-field")
