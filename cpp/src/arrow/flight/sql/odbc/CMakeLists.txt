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

if(APPLE)
  # CMAKE_FIND_LIBRARY_SUFFIXES.
  set(APPEND CMAKE_FIND_LIBRARY_SUFFIXES ".a")
endif()

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
add_subdirectory(tests)

arrow_install_all_headers("arrow/flight/sql/odbc")

# ODBC Release information
# Flight SQL ODBC version uses Arrow version
set(ODBC_PACKAGE_VERSION_MAJOR "${arrow_VERSION_MAJOR}")
set(ODBC_PACKAGE_VERSION_MINOR "${arrow_VERSION_MINOR}")
set(ODBC_PACKAGE_VERSION_PATCH "${arrow_VERSION_PATCH}")
set(ODBC_PACKAGE_NAME "Apache Arrow Flight SQL ODBC")
set(ODBC_PACKAGE_VENDOR "Apache Software Foundation")

set(ARROW_FLIGHT_SQL_ODBC_SRCS entry_points.cc odbc_api.cc)

if(WIN32)
  set(VER_FILEVERSION
      "${ODBC_PACKAGE_VERSION_MAJOR},${ODBC_PACKAGE_VERSION_MINOR},${ODBC_PACKAGE_VERSION_PATCH},0"
  )
  set(VER_FILEVERSION_STR
      ${ODBC_PACKAGE_VERSION_MAJOR}.${ODBC_PACKAGE_VERSION_MINOR}.${ODBC_PACKAGE_VERSION_PATCH}
  )
  set(VER_COMPANYNAME_STR ${ODBC_PACKAGE_VENDOR})
  set(VER_PRODUCTNAME_STR ${ODBC_PACKAGE_NAME})

  configure_file("install/windows/versioninfo.rc.in" "install/versioninfo.rc" @ONLY)

  list(APPEND ARROW_FLIGHT_SQL_ODBC_SRCS odbc.def install/versioninfo.rc)
endif()

# On Windows, dynmaic build for ODBC is supported.
# On unix systems, static build for ODBC is supported, all libraries are linked statically on unix.
if(WIN32)
  add_arrow_lib(arrow_flight_sql_odbc
                CMAKE_PACKAGE_NAME
                ArrowFlightSqlOdbc
                PKG_CONFIG_NAME
                arrow-flight-sql-odbc
                OUTPUTS
                ARROW_FLIGHT_SQL_ODBC_LIBRARIES
                SOURCES
                ${ARROW_FLIGHT_SQL_ODBC_SRCS}
                DEFINITIONS
                UNICODE
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
                SHARED_LINK_LIBS
                arrow_flight_sql_shared
                arrow_odbc_spi_impl
                SHARED_INSTALL_INTERFACE_LIBS
                ArrowFlight::arrow_flight_sql_shared
                STATIC_LINK_LIBS
                arrow_flight_sql_static
                STATIC_INSTALL_INTERFACE_LIBS
                ArrowFlight::arrow_flight_sql_static
                SHARED_PRIVATE_LINK_LIBS
                ODBC::ODBC
                ${ODBCINST})
else()
  # Unix
  add_arrow_lib(arrow_flight_sql_odbc
                CMAKE_PACKAGE_NAME
                ArrowFlightSqlOdbc
                PKG_CONFIG_NAME
                arrow-flight-sql-odbc
                OUTPUTS
                ARROW_FLIGHT_SQL_ODBC_LIBRARIES
                SOURCES
                ${ARROW_FLIGHT_SQL_ODBC_SRCS}
                DEFINITIONS
                UNICODE
                SHARED_LINK_FLAGS
                ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
                STATIC_LINK_LIBS
                iodbc
                ODBC::ODBC
                ${ODBCINST}
                SHARED_LINK_LIBS
                arrow_odbc_spi_impl)
endif()

foreach(LIB_TARGET ${ARROW_FLIGHT_SQL_ODBC_LIBRARIES})
  target_compile_definitions(${LIB_TARGET} PRIVATE ARROW_FLIGHT_SQL_ODBC_EXPORTING)
endforeach()

# Construct ODBC Windows installer. Only Release installer is supported
if(ARROW_FLIGHT_SQL_ODBC_INSTALLER)

  include(InstallRequiredSystemLibraries)

  set(CPACK_RESOURCE_FILE_LICENSE
      "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../LICENSE.txt")

  set(CPACK_PACKAGE_VERSION_MAJOR ${ODBC_PACKAGE_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${ODBC_PACKAGE_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${ODBC_PACKAGE_VERSION_PATCH})

  set(CPACK_PACKAGE_NAME ${ODBC_PACKAGE_NAME})
  # Make sure the MSI name contains only hyphens, not spaces
  string(REPLACE " " "-" CPACK_PACKAGE_NAME "${CPACK_PACKAGE_NAME}")
  set(CPACK_PACKAGE_VENDOR ${ODBC_PACKAGE_VENDOR})
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Apache Arrow Flight SQL ODBC Driver")
  set(CPACK_PACKAGE_CONTACT "dev@arrow.apache.org")

  # GH-47876 TODO: set up `arrow_flight_sql_odbc` component for macOS Installer
  # GH-47877 TODO: set up `arrow_flight_sql_odbc` component for Linux Installer
  if(WIN32)
    # Install ODBC and its Arrow dependencies
    install(PROGRAMS ${CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS}
            DESTINATION bin
            COMPONENT arrow_flight_sql_odbc)
    install(TARGETS arrow_shared
                    arrow_compute_shared
                    arrow_flight_shared
                    arrow_flight_sql_shared
                    arrow_flight_sql_odbc_shared
                    RUNTIME_DEPENDENCIES
                    PRE_EXCLUDE_REGEXES
                    "api-ms-.*"
                    "ext-ms-.*"
                    POST_EXCLUDE_REGEXES
                    ".*system32/.*\\.dll"
            RUNTIME DESTINATION bin COMPONENT arrow_flight_sql_odbc)

    set(CPACK_WIX_EXTRA_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-flight-sql-odbc.wxs")
    set(CPACK_WIX_PATCH_FILE
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-flight-sql-odbc-patch.xml")

    set(CPACK_WIX_UI_BANNER
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-wix-banner.bmp")
  endif()

  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  set(CPACK_COMPONENTS_ALL "arrow_flight_sql_odbc")

  if(WIN32)
    # WiX msi installer on Windows
    # CPack is compatible with WiX V.5 and V.6
    set(CPACK_GENERATOR "WIX")
    set(CPACK_WIX_VERSION 4)

    # Upgrade GUID is required to be unchanged for ODBC installer to upgrade
    set(CPACK_WIX_UPGRADE_GUID "DBF27A18-F8BF-423F-9E3A-957414D52C4B")
    set(CPACK_WIX_PRODUCT_GUID "279D087B-93B5-4DC3-BA69-BCF485022A26")
  endif()
  # GH-47876 TODO: create macOS Installer using cpack
  # GH-47877 TODO: create Linux Installer using cpack

  # Load CPack after all CPACK* variables are set
  include(CPack)
  cpack_add_component(arrow_flight_sql_odbc
                      DISPLAY_NAME "ODBC library"
                      DESCRIPTION "Apache Arrow Flight SQL ODBC library bin, required to install"
                      REQUIRED)
endif()
