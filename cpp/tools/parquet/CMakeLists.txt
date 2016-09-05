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

SET(LINK_LIBS
  snappystatic
  thriftstatic)

if (PARQUET_BUILD_EXECUTABLES)
  add_executable(parquet-dump-schema parquet-dump-schema.cc)
  target_link_libraries(parquet-dump-schema ${LINK_LIBS}
	parquet_static)

  add_executable(parquet_reader parquet_reader.cc)
  target_link_libraries(parquet_reader ${LINK_LIBS}
	parquet_static)

  add_executable(parquet-scan parquet-scan.cc)
  target_link_libraries(parquet-scan ${LINK_LIBS}
	parquet_static)
endif()
