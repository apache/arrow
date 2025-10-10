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

add_definitions(-DUNICODE=1)

add_subdirectory(odbc_impl)
add_subdirectory(tests)

arrow_install_all_headers("arrow/flight/sql/odbc")

# ODBC Release information
set(ODBC_PACKAGE_VERSION_MAJOR "1")
set(ODBC_PACKAGE_VERSION_MINOR "0")
set(ODBC_PACKAGE_VERSION_PATCH "0")
set(ODBC_PACKAGE_NAME "Apache Arrow Flight SQL ODBC")
set(ODBC_PACKAGE_VENDOR "Apache Arrow")

# Compile entry_points.cc before odbc_api.cc due to conflict from sql.h and flight/types.h
set(ARROW_FLIGHT_SQL_ODBC_SRCS odbc_api.cc entry_points.cc)

if(WIN32)
  set(VER_FILEVERSION
      "${ODBC_PACKAGE_VERSION_MAJOR},${ODBC_PACKAGE_VERSION_MINOR},${ODBC_PACKAGE_VERSION_PATCH},0"
  )
  set(VER_FILEVERSION_STR
      ${ODBC_PACKAGE_VERSION_MAJOR}.${ODBC_PACKAGE_VERSION_MINOR}.${ODBC_PACKAGE_VERSION_PATCH}
  )
  set(VER_COMPANYNAME_STR ${ODBC_PACKAGE_VENDOR})
  set(VER_PRODUCTNAME_STR ${ODBC_PACKAGE_NAME})

  configure_file("install/versioninfo.rc.in" "install/versioninfo.rc" @ONLY)

  list(APPEND ARROW_FLIGHT_SQL_ODBC_SRCS odbc.def install/versioninfo.rc)
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

# Construct ODBC Windows installer. Only Release installer is supported
if(ARROW_FLIGHT_SQL_ODBC_INSTALLER)

  include(InstallRequiredSystemLibraries)

  set(CPACK_RESOURCE_FILE_LICENSE
      "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../LICENSE.txt")
  # Tentative version 1.0.0
  set(CPACK_PACKAGE_VERSION_MAJOR ${ODBC_PACKAGE_VERSION_MAJOR})
  set(CPACK_PACKAGE_VERSION_MINOR ${ODBC_PACKAGE_VERSION_MINOR})
  set(CPACK_PACKAGE_VERSION_PATCH ${ODBC_PACKAGE_VERSION_PATCH})

  set(CPACK_PACKAGE_NAME ${ODBC_PACKAGE_NAME})
  set(CPACK_PACKAGE_VENDOR ${ODBC_PACKAGE_VENDOR})
  set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Apache Arrow Flight SQL ODBC Driver")
  set(CPACK_PACKAGE_CONTACT "#TODO arrow maintainers")

  # TODO: set up `flight_sql_odbc_lib` component for macOS Installer
  # TODO: set up `flight_sql_odbc_lib` component for Linux Installer
  if(WIN32)
    install(DIRECTORY "${BUILD_OUTPUT_ROOT_DIRECTORY}${CMAKE_BUILD_TYPE}/"
            DESTINATION bin
            COMPONENT flight_sql_odbc_lib
            FILES_MATCHING
            # Use regex for dll name patterns with versions
            PATTERN "abseil_dll.dll"
            PATTERN "arrow.dll"
            PATTERN "arrow_compute.dll"
            PATTERN "arrow_flight.dll"
            PATTERN "arrow_flight_sql.dll"
            PATTERN "arrow_flight_sql_odbc.dll"
            PATTERN "boost_locale*.dll"
            PATTERN "cares.dll"
            PATTERN "libcrypto*.dll"
            PATTERN "libprotobuf.dll"
            PATTERN "libssl*.dll"
            PATTERN "re2.dll"
            PATTERN "utf8proc.dll"
            PATTERN "zlib1.dll")

    set(CPACK_WIX_EXTRA_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/install/arrow-flight-sql-odbc.wxs")
    set(CPACK_WIX_PATCH_FILE
        "${CMAKE_CURRENT_SOURCE_DIR}/install/arrow-flight-sql-odbc-patch.xml")

    set(CPACK_WIX_UI_BANNER "${CMAKE_CURRENT_SOURCE_DIR}/install/arrow-wix-banner.bmp")
  endif()

  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  set(CPACK_COMPONENTS_ALL Unspecified)
  list(APPEND CPACK_COMPONENTS_ALL "flight_sql_odbc_lib")

  if(WIN32)
    # WiX msi installer on Windows
    # CPack is compatible with WiX V.5 and V.6
    set(CPACK_GENERATOR "WIX")
    set(CPACK_WIX_VERSION 4)

    # Upgrade GUID is required to be unchanged for ODBC installer to upgrade
    set(CPACK_WIX_UPGRADE_GUID "DBF27A18-F8BF-423F-9E3A-957414D52C4B")
  endif()
  # TODO: create macOS Installer using cpack
  # TODO: create Linux Installer using cpack

  # Load CPack after all CPACK* variables are set
  include(CPack)
  cpack_add_component(flight_sql_odbc_lib
                      DISPLAY_NAME "ODBC library"
                      DESCRIPTION "ODBC library bin, required to install"
                      REQUIRED)
endif()
