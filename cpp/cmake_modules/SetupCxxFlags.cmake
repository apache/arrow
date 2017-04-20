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

# Check if the target architecture and compiler supports some special
# instruction sets that would boost performance.
include(CheckCXXCompilerFlag)
# x86/amd64 compiler flags
CHECK_CXX_COMPILER_FLAG("-msse3" CXX_SUPPORTS_SSE3)
# power compiler flags
CHECK_CXX_COMPILER_FLAG("-maltivec" CXX_SUPPORTS_ALTIVEC)

# compiler flags that are common across debug/release builds

if (MSVC)
  # TODO(wesm): Change usages of C runtime functions that MSVC says are
  # insecure, like std::getenv
  add_definitions(-D_CRT_SECURE_NO_WARNINGS)

  if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # clang-cl
    set(CXX_COMMON_FLAGS "-EHsc")
  elseif(${CMAKE_CXX_COMPILER_VERSION} VERSION_LESS 19)
    message(FATAL_ERROR "Only MSVC 2015 (Version 19.0) and later are supported
    by Arrow. Found version ${CMAKE_CXX_COMPILER_VERSION}.")
  else()
    # Fix annoying D9025 warning
    string(REPLACE "/W3" "" CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}")

    # Set desired warning level (e.g. set /W4 for more warnings)
    set(CXX_COMMON_FLAGS "/W3")
  endif()
else()
  set(CXX_COMMON_FLAGS "-Wall -std=c++11")
endif()

# Only enable additional instruction sets if they are supported
if (CXX_SUPPORTS_SSE3 AND ARROW_SSE3)
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -msse3")
endif()

if (CXX_SUPPORTS_ALTIVEC AND ARROW_ALTIVEC)
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -maltivec")
endif()

if (APPLE)
  # Depending on the default OSX_DEPLOYMENT_TARGET (< 10.9), libstdc++ may be
  # the default standard library which does not support C++11. libc++ is the
  # default from 10.9 onward.
  set(CXX_COMMON_FLAGS "${CXX_COMMON_FLAGS} -stdlib=libc++")
endif()

# compiler flags for different build types (run 'cmake -DCMAKE_BUILD_TYPE=<type> .')
# For all builds:
# For CMAKE_BUILD_TYPE=Debug
#   -ggdb: Enable gdb debugging
# For CMAKE_BUILD_TYPE=FastDebug
#   Same as DEBUG, except with some optimizations on.
# For CMAKE_BUILD_TYPE=Release
#   -O3: Enable all compiler optimizations
#   Debug symbols are stripped for reduced binary size. Add
#   -DARROW_CXXFLAGS="-g" to add them
if (NOT MSVC)
  set(CXX_FLAGS_DEBUG "-ggdb -O0")
  set(CXX_FLAGS_FASTDEBUG "-ggdb -O1")
  set(CXX_FLAGS_RELEASE "-O3 -DNDEBUG")
endif()

set(CXX_FLAGS_PROFILE_GEN "${CXX_FLAGS_RELEASE} -fprofile-generate")
set(CXX_FLAGS_PROFILE_BUILD "${CXX_FLAGS_RELEASE} -fprofile-use")

# if no build build type is specified, default to debug builds
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif(NOT CMAKE_BUILD_TYPE)

string (TOUPPER ${CMAKE_BUILD_TYPE} CMAKE_BUILD_TYPE)

# Set compile flags based on the build type.
message("Configured for ${CMAKE_BUILD_TYPE} build (set with cmake -DCMAKE_BUILD_TYPE={release,debug,...})")
if ("${CMAKE_BUILD_TYPE}" STREQUAL "DEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_DEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "FASTDEBUG")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_FASTDEBUG}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "RELEASE")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_RELEASE}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_GEN")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_GEN}")
elseif ("${CMAKE_BUILD_TYPE}" STREQUAL "PROFILE_BUILD")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CXX_FLAGS_PROFILE_BUILD}")
else()
  message(FATAL_ERROR "Unknown build type: ${CMAKE_BUILD_TYPE}")
endif ()

message(STATUS "Build Type: ${CMAKE_BUILD_TYPE}")
