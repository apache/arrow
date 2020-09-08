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

if(DEFINED ENV{BOOST_ROOT} OR DEFINED BOOST_ROOT)
  # In older versions of CMake (such as 3.2), the system paths for Boost will
  # be looked in first even if we set $BOOST_ROOT or pass -DBOOST_ROOT
  set(Boost_NO_SYSTEM_PATHS ON)
endif()

set(BoostAlt_FIND_VERSION_OPTIONS)
if(BoostAlt_FIND_VERSION)
  list(APPEND BoostAlt_FIND_VERSION_OPTIONS ${BoostAlt_FIND_VERSION})
endif()
if(BoostAlt_FIND_REQUIRED)
  list(APPEND BoostAlt_FIND_VERSION_OPTIONS REQUIRED)
endif()
if(BoostAlt_FIND_QUIETLY)
  list(APPEND BoostAlt_FIND_VERSION_OPTIONS QUIET)
endif()

if(ARROW_BOOST_USE_SHARED)
  # Find shared Boost libraries.
  set(Boost_USE_STATIC_LIBS OFF)
  set(BUILD_SHARED_LIBS_KEEP ${BUILD_SHARED_LIBS})
  set(BUILD_SHARED_LIBS ON)

  find_package(Boost ${BoostAlt_FIND_VERSION_OPTIONS}
               COMPONENTS regex system filesystem)
  set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_KEEP})
  unset(BUILD_SHARED_LIBS_KEEP)
else()
  # Find static boost headers and libs
  # TODO Differentiate here between release and debug builds
  set(Boost_USE_STATIC_LIBS ON)
  find_package(Boost ${BoostAlt_FIND_VERSION_OPTIONS}
               COMPONENTS regex system filesystem)
endif()

if(Boost_FOUND)
  set(BoostAlt_FOUND ON)
  if(MSVC_TOOLCHAIN)
    # disable autolinking in boost
    add_definitions(-DBOOST_ALL_NO_LIB)
    if(ARROW_BOOST_USE_SHARED)
      # force all boost libraries to dynamic link
      add_definitions(-DBOOST_ALL_DYN_LINK)
    endif()
  endif()
else()
  set(BoostAlt_FOUND OFF)
endif()
