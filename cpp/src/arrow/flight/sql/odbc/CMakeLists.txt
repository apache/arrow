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
  if(WIN32 OR APPLE)
    # GH-49552 TODO: Enable Linux test build
    add_subdirectory(tests)
  endif()
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

# On Windows, dynmaic build for ODBC is supported.
# On unix systems, static build for ODBC is supported, all libraries are linked statically on unix.
set(ARROW_FLIGHT_SQL_ODBC_DEPENDENCIES "")
set(ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS "")
set(ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS "")
set(ARROW_FLIGHT_SQL_ODBC_SHARED_PRIVATE_LINK_LIBS "")
if(WIN32)
  set(ARROW_FLIGHT_SQL_ODBC_DEPENDENCIES arrow_flight_sql)
  set(ARROW_FLIGHT_SQL_ODBC_SHARED_LINK_LIBS arrow_flight_sql_shared arrow_odbc_spi_impl)
  set(ARROW_FLIGHT_SQL_ODBC_STATIC_LINK_LIBS arrow_flight_sql_static)
  list(APPEND ARROW_FLIGHT_SQL_ODBC_SHARED_INSTALL_INTERFACE_LIBS
       ArrowFlight::arrow_flight_sql_shared)
  list(APPEND ARROW_FLIGHT_SQL_ODBC_STATIC_INSTALL_INTERFACE_LIBS
       ArrowFlight::arrow_flight_sql_static)
  list(APPEND ARROW_FLIGHT_SQL_ODBC_SHARED_PRIVATE_LINK_LIBS ODBC::ODBC ${ODBCINST})
else()
  # Unix
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
              DEPENDENCIES
              ${ARROW_FLIGHT_SQL_ODBC_DEPENDENCIES}
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
  else()
    set(ODBC_UNIX_FILE_NAME
        "ArrowFlightSqlOdbc-${CPACK_PACKAGE_VERSION_MAJOR}.${ODBC_PACKAGE_VERSION_MINOR}.${ODBC_PACKAGE_VERSION_PATCH}"
    )
    if(APPLE)
      set(CPACK_PACKAGE_FILE_NAME "${ODBC_UNIX_FILE_NAME}")
      set(CPACK_PACKAGE_INSTALL_DIRECTORY "${CPACK_PACKAGE_NAME}")

      set(CPACK_SET_DESTDIR ON)
      set(CPACK_INSTALL_PREFIX "/Library/ODBC")
      # Register ODBC after install
      set(CPACK_POSTFLIGHT_ARROW_FLIGHT_SQL_ODBC_SCRIPT
          "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/postinstall")
      set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/README.txt")
      set(CPACK_RESOURCE_FILE_WELCOME
          "${CMAKE_CURRENT_SOURCE_DIR}/install/mac/welcome.txt")

      set(ODBC_INSTALL_DIR "arrow-odbc/lib")
      set(ODBC_DOC_INSTALL_DIR "arrow-odbc/doc")
    else()
      # Linux
      set(CPACK_PACKAGE_INSTALL_DIRECTORY "${CPACK_PACKAGE_NAME}")
      if(${ODBC_PACKAGE_FORMAT} STREQUAL "DEB")
        set(CPACK_DEBIAN_PACKAGE_ARCHITECTURE amd64)
        set(CPACK_GENERATOR DEB)
        set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
            "${CMAKE_CURRENT_SOURCE_DIR}/install/linux/postinst")
        set(CPACK_DEBIAN_FILE_NAME "${ODBC_UNIX_FILE_NAME}.deb")
        set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${CPACK_PACKAGE_CONTACT}")
        set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS OFF)
        # Exclude Unspecified components
        set(CPACK_DEB_COMPONENT_INSTALL ON)
        set(CPACK_COMPONENTS_GROUPING ALL_COMPONENTS_IN_ONE)
      elseif(${ODBC_PACKAGE_FORMAT} STREQUAL "RPM")
        set(CPACK_RPM_PACKAGE_ARCHITECTURE x86_64)
        set(CPACK_GENERATOR RPM)
        set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE
            "${CMAKE_CURRENT_SOURCE_DIR}/install/linux/postinst")
        set(CPACK_RPM_FILE_NAME "${ODBC_UNIX_FILE_NAME}.rpm")
        # Disable dependency check as ODBC embeds all third party dependencies
        set(CPACK_RPM_PACKAGE_AUTOREQPROV "no")
        set(CPACK_COMPONENTS_ALL_IN_ONE_PACKAGE 1)
        set(CPACK_RPM_COMPONENT_INSTALL ON)
      else()
        message(FATAL_ERROR "ODBC_PACKAGE_FORMAT '${ODBC_PACKAGE_FORMAT}' must be DEB or RPM for Linux installer."
        )
      endif()

      # By default, Linux installs under /usr
      set(ODBC_INSTALL_DIR "lib64/arrow-odbc/lib")
      set(ODBC_DOC_INSTALL_DIR "lib64/arrow-odbc/doc")
    endif()

    # Install ODBC
    install(TARGETS arrow_flight_sql_odbc_shared
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT arrow_flight_sql_odbc)

    # Install temporary driver registration scripts, they will be removed after driver registration is complete
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/install/unix/install_odbc.sh"
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT arrow_flight_sql_odbc
            PERMISSIONS OWNER_EXECUTE
                        OWNER_WRITE
                        OWNER_READ
                        GROUP_EXECUTE
                        GROUP_READ
                        WORLD_EXECUTE
                        WORLD_READ)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/install/unix/install_odbc_ini.sh"
            DESTINATION "${ODBC_INSTALL_DIR}"
            COMPONENT arrow_flight_sql_odbc
            PERMISSIONS OWNER_EXECUTE
                        OWNER_WRITE
                        OWNER_READ
                        GROUP_EXECUTE
                        GROUP_READ
                        WORLD_EXECUTE
                        WORLD_READ)

    # Install documentation files
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/../../../../../../LICENSE.txt"
            DESTINATION "${ODBC_DOC_INSTALL_DIR}"
            COMPONENT Docs)
    install(FILES "${CMAKE_CURRENT_SOURCE_DIR}/connection-options.md"
            DESTINATION "${ODBC_DOC_INSTALL_DIR}"
            COMPONENT Docs)
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
  else()
    # macOS and Linux
    list(APPEND CPACK_COMPONENTS_ALL Docs)
    if(APPLE)
      set(CPACK_GENERATOR "productbuild")
    endif()
  endif()
  # GH-47877 TODO: create Linux Installer using cpack

  # Load CPack after all CPACK* variables are set
  include(CPack)
  cpack_add_component(arrow_flight_sql_odbc
                      DISPLAY_NAME "ODBC library"
                      DESCRIPTION "Apache Arrow Flight SQL ODBC library bin, required to install"
                      REQUIRED)
  if(UNIX)
    # On macOS and Linux, provide connection string documentation since users need to manually enter DSN keys.
    cpack_add_component(Docs
                        DISPLAY_NAME "Documentation"
                        DESCRIPTION "Documentation for Apache Arrow Flight SQL ODBC Driver"
    )
  endif()
endif()
