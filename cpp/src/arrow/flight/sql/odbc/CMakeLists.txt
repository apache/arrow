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

if(WIN32)
  if(MSVC_VERSION GREATER_EQUAL 1900)
    set(ODBCINST legacy_stdio_definitions odbccp32 shlwapi)
  elseif(MINGW)
    set(ODBCINST odbccp32 shlwapi)
  endif()
elseif(APPLE)
  set(ODBCINST iodbc iodbcinst)
else()
  set(ODBCINST odbcinst)
endif()

add_subdirectory(odbc_impl)
if(ARROW_BUILD_TESTS)
  add_subdirectory(tests)
endif()

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

# Initialize install interface libs (only set in Windows static build)
set(ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS "")
set(ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS "")
set(ARROW_FLIGHT_SQL_ODBC_SHARED_PRIVATE_LINK_LIBS "")

if(WIN32)
  if(ARROW_BUILD_STATIC)
    # Static Arrow deps on Windows — self-contained ODBC DLL
    set(ARROW_FLIGHT_SQL_ODBC_SHARED_LINK_LIBS arrow_flight_sql_static
                                               arrow_odbc_spi_impl)
    set(ARROW_FLIGHT_SQL_ODBC_STATIC_LINK_LIBS arrow_flight_sql_static)
    list(APPEND ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS
         ArrowFlight::arrow_flight_sql_static)
    list(APPEND ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS
         ArrowFlight::arrow_flight_sql_static)
  else()
    set(ARROW_FLIGHT_SQL_ODBC_SHARED_LINK_LIBS arrow_flight_sql_shared
                                               arrow_odbc_spi_impl)
    set(ARROW_FLIGHT_SQL_ODBC_STATIC_LINK_LIBS arrow_flight_sql_static)
    list(APPEND ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS
         ArrowFlight::arrow_flight_sql_shared)
    list(APPEND ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS
         ArrowFlight::arrow_flight_sql_static)
  endif()
  list(APPEND ARROW_FLIGHT_SQL_ODBC_SHARED_PRIVATE_LINK_LIBS ODBC::ODBC ${ODBCINST})
else()
  # Unix/macOS: driver dynamically links arrow_flight_sql so the driver and test
  # binary share the same grpc/arrow_flight runtime.
  set(ARROW_FLIGHT_SQL_ODBC_SHARED_LINK_LIBS arrow_odbc_spi_impl)
  set(ARROW_FLIGHT_SQL_ODBC_STATIC_LINK_LIBS ODBC::ODBC ${ODBCINST})
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
              DEFINITIONS
              UNICODE
              SHARED_LINK_FLAGS
              ${ARROW_VERSION_SCRIPT_FLAGS} # Defined in cpp/arrow/CMakeLists.txt
              SHARED_LINK_LIBS
              ${ARROW_FLIGHT_SQL_ODBC_SHARED_LINK_LIBS}
              SHARED_INSTALL_INTERFACE_LIBS
              ${ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS}
              STATIC_LINK_LIBS
              ${ARROW_FLIGHT_SQL_ODBC_STATIC_LINK_LIBS}
              STATIC_INSTALL_INTERFACE_LIBS
              ${ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS}
              SHARED_PRIVATE_LINK_LIBS
              ${ARROW_FLIGHT_SQL_ODBC_SHARED_PRIVATE_LINK_LIBS})

# NOTE-49585: Workaround for issue found in GH-49585. See "NOTE-49585" in
# ../../CMakeLists.txt.
if(WIN32
   AND CMAKE_BUILD_TYPE STREQUAL "Debug"
   AND ARROW_FLIGHT_TEST_LINKAGE STREQUAL "static")
  foreach(LIB_TARGET ${ARROW_FLIGHT_SQL_ODBC_LIBRARIES})
    target_link_options(${LIB_TARGET} PRIVATE "/FORCE:MULTIPLE")
  endforeach()
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

  if(WIN32)
    install(PROGRAMS ${CMAKE_INSTALL_SYSTEM_RUNTIME_LIBS}
            DESTINATION bin
            COMPONENT ArrowFlightSQLODBC)
    install(TARGETS arrow_flight_sql_odbc_shared
                    RUNTIME_DEPENDENCIES
                    PRE_EXCLUDE_REGEXES
                    "api-ms-.*"
                    "ext-ms-.*"
                    POST_EXCLUDE_REGEXES
                    ".*system32/.*\\.dll"
            RUNTIME DESTINATION bin COMPONENT ArrowFlightSQLODBC)

    set(CPACK_WIX_EXTRA_SOURCES
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-flight-sql-odbc.wxs")
    set(CPACK_WIX_PATCH_FILE
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-flight-sql-odbc-patch.xml")

    set(CPACK_WIX_UI_BANNER
        "${CMAKE_CURRENT_SOURCE_DIR}/install/windows/arrow-wix-banner.bmp")
  else()
    if(APPLE)
      set(CPACK_PACKAGE_FILE_NAME
          "ArrowFlightSQLODBC-${CPACK_PACKAGE_VERSION_MAJOR}.${ODBC_PACKAGE_VERSION_MINOR}.${ODBC_PACKAGE_VERSION_PATCH}"
      )
      set(CPACK_PACKAGE_INSTALL_DIRECTORY "${CPACK_PACKAGE_NAME}")

      set(CPACK_SET_DESTDIR ON)
      set(CPACK_INSTALL_PREFIX "/Library/ODBC")
      # Register ODBC after install
      set(CPACK_POSTFLIGHT_ARROWFLIGHTSQLODBC_SCRIPT
          "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/postinstall")
      set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/README.txt")
      set(CPACK_RESOURCE_FILE_WELCOME
          "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/welcome.txt")

      set(ODBC_INSTALL_DIR "arrow-odbc/lib")
      set(ODBC_DOC_INSTALL_DIR "arrow-odbc/doc")
    else()
      # Linux
      # GH-49595: TODO implement DEB installer
      # GH-47977: TODO implement RPM installer
      message(STATUS "ODBC_PACKAGE_FORMAT DEB not implemented, see GH-49595")
      message(STATUS "ODBC_PACKAGE_FORMAT RPM not implemented, see GH-47977")
    endif()

    # Install ODBC
    install(TARGETS arrow_flight_sql_odbc_shared
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT ArrowFlightSQLODBC)

    # Install temporary driver registration scripts, they will be removed after driver registration is complete
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/install/unix/install_odbc.sh"
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT ArrowFlightSQLODBC
            PERMISSIONS OWNER_EXECUTE
                        OWNER_WRITE
                        OWNER_READ
                        GROUP_EXECUTE
                        GROUP_READ
                        WORLD_EXECUTE
                        WORLD_READ)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/install/unix/install_odbc_ini.sh"
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT ArrowFlightSQLODBC
            PERMISSIONS OWNER_EXECUTE
                        OWNER_WRITE
                        OWNER_READ
                        GROUP_EXECUTE
                        GROUP_READ
                        WORLD_EXECUTE
                        WORLD_READ)

    # Install documentation files
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../LICENSE.txt"
                  "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../NOTICE.txt"
                  "${CMAKE_CURRENT_SOURCE_DIR}/connection-options.md"
            DESTINATION "${ODBC_DOC_INSTALL_DIR}"
            COMPONENT Docs)
  endif()

  get_cmake_property(CPACK_COMPONENTS_ALL COMPONENTS)
  set(CPACK_COMPONENTS_ALL "ArrowFlightSQLODBC" "Docs")

  if(WIN32)
    # WiX msi installer on Windows
    # CPack is compatible with WiX V.5 and V.6
    set(CPACK_GENERATOR "WIX")
    set(CPACK_WIX_VERSION 4)

    # Upgrade GUID is required to be unchanged for ODBC installer to upgrade
    set(CPACK_WIX_UPGRADE_GUID "DBF27A18-F8BF-423F-9E3A-957414D52C4B")
    set(CPACK_WIX_PRODUCT_GUID "279D087B-93B5-4DC3-BA69-BCF485022A26")
  elseif(APPLE)
    # macOS and Linux
    set(CPACK_GENERATOR "productbuild")
  endif()
  # GH-47877 TODO: create Linux Installer using cpack

  # Load CPack after all CPACK* variables are set
  include(CPack)
  cpack_add_component(ArrowFlightSQLODBC
                      DISPLAY_NAME "ODBC library"
                      DESCRIPTION "Apache Arrow Flight SQL ODBC library bin, required to install"
                      REQUIRED)
  cpack_add_component(Docs
                      DISPLAY_NAME "Documentation"
                      DESCRIPTION "Documentation for Apache Arrow Flight SQL ODBC Driver"
                      REQUIRED)
endif()
