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

# Find uriparser.
#
# Debian, Ubuntu and several other distributions ship a liburiparser-dev that
# provides only a pkg-config file, while upstream (and vcpkg, conda-forge)
# also install a CMake package configuration. Try the CMake config first, then
# fall back to pkg-config.

if(uriparserAlt_FOUND)
  return()
endif()

set(find_package_args)
if(uriparserAlt_FIND_VERSION)
  list(APPEND find_package_args ${uriparserAlt_FIND_VERSION})
endif()
if(uriparserAlt_FIND_QUIETLY)
  list(APPEND find_package_args QUIET)
endif()
find_package(uriparser ${find_package_args} CONFIG)
if(uriparser_FOUND)
  set(uriparserAlt_VERSION "${uriparser_VERSION}")
  set(uriparserAlt_FOUND TRUE)
  return()
endif()

set(uriparser_pkg_config_args GLOBAL IMPORTED_TARGET)
if(uriparserAlt_FIND_QUIETLY)
  find_package(PkgConfig QUIET)
  list(APPEND uriparser_pkg_config_args QUIET)
else()
  find_package(PkgConfig)
endif()
list(APPEND uriparser_pkg_config_args liburiparser)
pkg_check_modules(uriparser_PC ${uriparser_pkg_config_args})
if(uriparser_PC_FOUND)
  set(uriparserAlt_VERSION "${uriparser_PC_VERSION}")
endif()

find_package_handle_standard_args(uriparserAlt
                                  REQUIRED_VARS uriparser_PC_FOUND
                                  VERSION_VAR uriparserAlt_VERSION)

if(uriparserAlt_FOUND AND NOT TARGET uriparser::uriparser)
  add_library(uriparser::uriparser ALIAS PkgConfig::uriparser_PC)
endif()
