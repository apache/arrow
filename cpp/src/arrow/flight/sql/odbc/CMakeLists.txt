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

# Use C++ 20 for ODBC and its subdirectory
# GH-44792: Arrow will switch to C++ 20
set(CMAKE_CXX_STANDARD 20)

add_custom_target(arrow_flight_sql_odbc)

if(WIN32)
  if(MSVC_VERSION GREATER_EQUAL 1900)
    set(ODBCINST legacy_stdio_definitions odbccp32 shlwapi)
  elseif(MINGW)
    set(ODBCINST odbccp32 shlwapi)
  endif()
elseif(APPLE)
  set(ODBCINST iodbcinst)
else()
  set(ODBCINST odbcinst)
endif()

add_subdirectory(odbc_impl)

arrow_install_all_headers("arrow/flight/sql/odbc")

set(ARROW_FLIGHT_SQL_ODBC_SRCS entry_points.cc odbc_api.cc)

if(WIN32)
  list(APPEND ARROW_FLIGHT_SQL_ODBC_SRCS odbc.def)
endif()

add_arrow_lib(arrow_flight_sql_odbc
              CMAKE_PACKAGE_NAME
              ArrowFlightSqlOdbc
              PKG_CONFIG_NAME
              arrow-flight-sql-odbc
              OUTPUTS
              ARROW_FLIGHT_SQL_ODBC_LIBRARIES
              SOURCES
              ${ARROW_FLIGHT_SQL_ODBC_SRCS}
              DEPENDENCIES
              arrow_flight_sql
              DEFINITIONS
              FMT_HEADER_ONLY
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              arrow_flight_sql_shared
              SHARED_INSTALL_INTERFACE_LIBS
              ArrowFlight::arrow_flight_sql_shared
              STATIC_LINK_LIBS
              arrow_flight_sql_static
              STATIC_INSTALL_INTERFACE_LIBS
              ArrowFlight::arrow_flight_sql_static
              SHARED_PRIVATE_LINK_LIBS
              ODBC::ODBC
              ${ODBCINST}
              arrow_odbc_spi_impl)

foreach(LIB_TARGET ${ARROW_FLIGHT_SQL_ODBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_SQL_ODBC_EXPORTING)
endforeach()
