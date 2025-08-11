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

include_directories(include)

# Ensure fmt is loaded as header only
add_compile_definitions(FMT_HEADER_ONLY)

add_library(odbcabstraction
            include/odbcabstraction/calendar_utils.h
            include/odbcabstraction/diagnostics.h
            include/odbcabstraction/error_codes.h
            include/odbcabstraction/exceptions.h
            include/odbcabstraction/logger.h
            include/odbcabstraction/platform.h
            include/odbcabstraction/spd_logger.h
            include/odbcabstraction/types.h
            include/odbcabstraction/utils.h
            include/odbcabstraction/odbc_impl/attribute_utils.h
            include/odbcabstraction/odbc_impl/encoding_utils.h
            include/odbcabstraction/odbc_impl/odbc_connection.h
            include/odbcabstraction/odbc_impl/odbc_descriptor.h
            include/odbcabstraction/odbc_impl/odbc_environment.h
            include/odbcabstraction/odbc_impl/odbc_handle.h
            include/odbcabstraction/odbc_impl/odbc_statement.h
            include/odbcabstraction/odbc_impl/type_utilities.h
            include/odbcabstraction/spi/connection.h
            include/odbcabstraction/spi/driver.h
            include/odbcabstraction/spi/result_set.h
            include/odbcabstraction/spi/result_set_metadata.h
            include/odbcabstraction/spi/statement.h
            calendar_utils.cc
            diagnostics.cc
            encoding.cc
            exceptions.cc
            logger.cc
            spd_logger.cc
            utils.cc
            ../../../../vendored/whereami/whereami.cc
            odbc_impl/odbc_connection.cc
            odbc_impl/odbc_descriptor.cc
            odbc_impl/odbc_environment.cc
            odbc_impl/odbc_statement.cc)
target_include_directories(odbcabstraction PUBLIC ${CMAKE_CURRENT_LIST_DIR}/include)
target_link_libraries(odbcabstraction PUBLIC ${ODBC_LIBRARIES} Boost::headers)

set_target_properties(odbcabstraction
                      PROPERTIES ARCHIVE_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
                                 LIBRARY_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib
                                 RUNTIME_OUTPUT_DIRECTORY
                                 ${CMAKE_BINARY_DIR}/$<CONFIG>/lib)

include(FetchContent)
fetchcontent_declare(spdlog
                     URL https://github.com/gabime/spdlog/archive/76fb40d95455f249bd70824ecfcae7a8f0930fa3.zip
                         CONFIGURE_COMMAND
                         ""
                         BUILD_COMMAND
                         "")
fetchcontent_getproperties(spdlog)
if(NOT spdlog_POPULATED)
  fetchcontent_populate(spdlog)
endif()

add_library(spdlog INTERFACE)
target_include_directories(spdlog INTERFACE ${spdlog_SOURCE_DIR}/include)

target_link_libraries(odbcabstraction PUBLIC spdlog)
