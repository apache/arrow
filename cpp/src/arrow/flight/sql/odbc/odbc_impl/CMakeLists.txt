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

add_library(arrow_odbc_spi_impl
            accessors/binary_array_accessor.cc
            accessors/binary_array_accessor.h
            accessors/boolean_array_accessor.cc
            accessors/boolean_array_accessor.h
            accessors/common.h
            accessors/date_array_accessor.cc
            accessors/date_array_accessor.h
            accessors/decimal_array_accessor.cc
            accessors/decimal_array_accessor.h
            accessors/primitive_array_accessor.cc
            accessors/primitive_array_accessor.h
            accessors/string_array_accessor.cc
            accessors/string_array_accessor.h
            accessors/time_array_accessor.cc
            accessors/time_array_accessor.h
            accessors/timestamp_array_accessor.cc
            accessors/timestamp_array_accessor.h
            address_info.cc
            address_info.h
            attribute_utils.h
            blocking_queue.h
            calendar_utils.cc
            calendar_utils.h
            config/configuration.cc
            config/configuration.h
            config/connection_string_parser.cc
            config/connection_string_parser.h
            diagnostics.cc
            diagnostics.h
            encoding.cc
            encoding.h
            encoding_utils.h
            error_codes.h
            exceptions.cc
            exceptions.h
            flight_sql_auth_method.cc
            flight_sql_auth_method.h
            flight_sql_connection.cc
            flight_sql_connection.h
            flight_sql_driver.cc
            flight_sql_driver.h
            flight_sql_get_tables_reader.cc
            flight_sql_get_tables_reader.h
            flight_sql_get_type_info_reader.cc
            flight_sql_get_type_info_reader.h
            flight_sql_result_set.cc
            flight_sql_result_set.h
            flight_sql_result_set_accessors.cc
            flight_sql_result_set_accessors.h
            flight_sql_result_set_column.cc
            flight_sql_result_set_column.h
            flight_sql_result_set_metadata.cc
            flight_sql_result_set_metadata.h
            flight_sql_ssl_config.cc
            flight_sql_ssl_config.h
            flight_sql_statement.cc
            flight_sql_statement.h
            flight_sql_statement_get_columns.cc
            flight_sql_statement_get_columns.h
            flight_sql_statement_get_tables.cc
            flight_sql_statement_get_tables.h
            flight_sql_statement_get_type_info.cc
            flight_sql_statement_get_type_info.h
            flight_sql_stream_chunk_buffer.cc
            flight_sql_stream_chunk_buffer.h
            get_info_cache.cc
            get_info_cache.h
            json_converter.cc
            json_converter.h
            odbc_connection.cc
            odbc_connection.h
            odbc_descriptor.cc
            odbc_descriptor.h
            odbc_environment.cc
            odbc_environment.h
            odbc_handle.h
            odbc_statement.cc
            odbc_statement.h
            platform.h
            record_batch_transformer.cc
            record_batch_transformer.h
            scalar_function_reporter.cc
            scalar_function_reporter.h
            spi/connection.h
            spi/driver.h
            spi/result_set.h
            spi/result_set_metadata.h
            spi/statement.h
            system_trust_store.cc
            system_trust_store.h
            types.h
            type_fwd.h
            type_utilities.h
            util.cc
            util.h)
target_compile_definitions(arrow_odbc_spi_impl PUBLIC UNICODE)
target_include_directories(arrow_odbc_spi_impl PUBLIC include include/odbc_impl)
target_include_directories(arrow_odbc_spi_impl PUBLIC ${CMAKE_CURRENT_LIST_DIR})

if(WIN32)
  target_sources(arrow_odbc_spi_impl
                 PRIVATE ui/add_property_window.cc
                         ui/add_property_window.h
                         ui/custom_window.cc
                         ui/custom_window.h
                         ui/dsn_configuration_window.cc
                         ui/dsn_configuration_window.h
                         ui/window.cc
                         ui/window.h
                         win_system_dsn.cc
                         system_dsn.cc
                         system_dsn.h)
endif()

if(APPLE)
  target_include_directories(arrow_odbc_spi_impl SYSTEM BEFORE PUBLIC ${ODBC_INCLUDE_DIR})
  target_link_libraries(arrow_odbc_spi_impl
                        PUBLIC arrow_flight_sql_shared arrow_compute_shared Boost::locale
                               iodbc)
else()
  find_package(ODBC REQUIRED)
  target_include_directories(arrow_odbc_spi_impl PUBLIC ${ODBC_INCLUDE_DIR})
  target_link_libraries(arrow_odbc_spi_impl
                        PUBLIC arrow_flight_sql_shared arrow_compute_shared Boost::locale
                               ${ODBCINST})
endif()

set_target_properties(arrow_odbc_spi_impl
                      PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
                                 LIBRARY_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
                                 RUNTIME_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib)

# CLI
add_executable(arrow_odbc_spi_impl_cli main.cc)
set_target_properties(arrow_odbc_spi_impl_cli
                      PROPERTIES RUNTIME_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/bin)
target_link_libraries(arrow_odbc_spi_impl_cli arrow_odbc_spi_impl)

# Unit tests
add_arrow_test(odbc_spi_impl_test
               SOURCES
               accessors/boolean_array_accessor_test.cc
               accessors/binary_array_accessor_test.cc
               accessors/date_array_accessor_test.cc
               accessors/decimal_array_accessor_test.cc
               accessors/primitive_array_accessor_test.cc
               accessors/string_array_accessor_test.cc
               accessors/time_array_accessor_test.cc
               accessors/timestamp_array_accessor_test.cc
               flight_sql_connection_test.cc
               flight_sql_stream_chunk_buffer_test.cc
               parse_table_types_test.cc
               json_converter_test.cc
               record_batch_transformer_test.cc
               util_test.cc
               EXTRA_LINK_LIBS
               arrow_odbc_spi_impl
               arrow_flight_testing_shared)
