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

ADD_ARROW_TEST(io-buffered-test)
ADD_ARROW_TEST(io-compressed-test)
ADD_ARROW_TEST(io-file-test)

if (ARROW_HDFS AND NOT ARROW_BOOST_HEADER_ONLY)
  ADD_ARROW_TEST(io-hdfs-test NO_VALGRIND)
endif()

ADD_ARROW_TEST(io-memory-test)
ADD_ARROW_TEST(io-readahead-test)

ADD_ARROW_BENCHMARK(io-file-benchmark)
ADD_ARROW_BENCHMARK(io-memory-benchmark)

# Headers: top level
install(FILES
  api.h
  buffered.h
  compressed.h
  file.h
  hdfs.h
  interfaces.h
  memory.h
  readahead.h
  DESTINATION "${CMAKE_INSTALL_INCLUDEDIR}/arrow/io")
