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

add_executable(parquet_low_level_example low_level_api/reader_writer.cc)
add_executable(parquet_low_level_example2 low_level_api/reader_writer2.cc)
add_executable(parquet_arrow_example parquet_arrow/reader_writer.cc)
add_executable(parquet_stream_api_example parquet_stream_api/stream_reader_writer.cc)
target_include_directories(parquet_low_level_example PRIVATE low_level_api/)
target_include_directories(parquet_low_level_example2 PRIVATE low_level_api/)

# The variables in these files are for illustration purposes
set(PARQUET_EXAMPLES_WARNING_SUPPRESSIONS low_level_api/reader_writer.cc
                                          low_level_api/reader_writer2.cc)

if(PARQUET_REQUIRE_ENCRYPTION)
  add_executable(parquet_encryption_example low_level_api/encryption_reader_writer.cc)
  add_executable(parquet_encryption_example_all_crypto_options
                 low_level_api/encryption_reader_writer_all_crypto_options.cc)
  target_include_directories(parquet_encryption_example PRIVATE low_level_api/)
  target_include_directories(parquet_encryption_example_all_crypto_options
                             PRIVATE low_level_api/)

  set(PARQUET_EXAMPLES_WARNING_SUPPRESSIONS
      ${PARQUET_EXAMPLES_WARNING_SUPPRESSIONS} low_level_api/encryption_reader_writer.cc
      low_level_api/encryption_reader_writer_all_crypto_options.cc)

endif()

if(UNIX)
  foreach(FILE ${PARQUET_EXAMPLES_WARNING_SUPPRESSIONS})
    set_property(SOURCE ${FILE}
                 APPEND_STRING
                 PROPERTY COMPILE_FLAGS "-Wno-unused-variable")
  endforeach()
endif()

# Prefer shared linkage but use static if shared build is deactivated
if(ARROW_BUILD_SHARED)
  set(PARQUET_EXAMPLE_LINK_LIBS parquet_shared)
else()
  set(PARQUET_EXAMPLE_LINK_LIBS parquet_static)
endif()

target_link_libraries(parquet_arrow_example ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet_low_level_example ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet_low_level_example2 ${PARQUET_EXAMPLE_LINK_LIBS})
target_link_libraries(parquet_stream_api_example ${PARQUET_EXAMPLE_LINK_LIBS})

if(PARQUET_REQUIRE_ENCRYPTION)
  target_link_libraries(parquet_encryption_example ${PARQUET_EXAMPLE_LINK_LIBS})
  target_link_libraries(parquet_encryption_example_all_crypto_options
                        ${PARQUET_EXAMPLE_LINK_LIBS})
endif()

add_dependencies(parquet
                 parquet_low_level_example
                 parquet_low_level_example2
                 parquet_arrow_example
                 parquet_stream_api_example)

if(PARQUET_REQUIRE_ENCRYPTION)
  add_dependencies(parquet parquet_encryption_example
                   parquet_encryption_example_all_crypto_options)
endif()
