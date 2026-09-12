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

# Resolve the ARROW_USE_STD_CHRONO option (AUTO, ON or OFF) into a boolean.
#
# GH-51267 tracks removing the vendored datetime fallback once all supported
# toolchains provide working C++20 chrono timezone support. Until then:
# - AUTO keeps the historical platform default: std::chrono is used on Windows
#   toolchains whose standard library provides working C++20 chrono timezone
#   support, and the vendored datetime fallback is used everywhere else.
# - ON opts into std::chrono unconditionally, failing the configure step when
#   the toolchain does not provide working C++20 chrono timezone support.
# - OFF always uses the vendored datetime fallback.
#
# The resolved value is consumed via arrow/util/config.h (ARROW_USE_STD_CHRONO)
# by arrow/util/chrono_internal.h and to decide whether the vendored datetime
# implementation is built.

include(CheckCXXSourceCompiles)

# When Arrow is consumed as a CMake subproject, ARROW_USE_STD_CHRONO is not
# defined; skip detection and let arrow/util/chrono_internal.h fall back to its
# default backend selection (vendored datetime fallback).
if(DEFINED ARROW_USE_STD_CHRONO)
  if(NOT "${ARROW_USE_STD_CHRONO}" MATCHES "^(AUTO|ON|OFF)$")
    message(FATAL_ERROR "ARROW_USE_STD_CHRONO must be one of AUTO, ON or OFF "
                        "(got \"${ARROW_USE_STD_CHRONO}\")")
  endif()

  set(_ARROW_STD_CHRONO_TEST_SOURCE
      "
#include <chrono>
#if !defined(__cpp_lib_chrono) || __cpp_lib_chrono < 201907L
#  error \"C++20 chrono timezone support (__cpp_lib_chrono >= 201907L) is unavailable\"
#endif
int main() { return 0; }
")

  function(_arrow_check_std_chrono_support out_var)
    # check_cxx_source_compiles() compiles with the toolchain default standard,
    # so force C++20 explicitly for this probe.
    if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
      set(CMAKE_REQUIRED_FLAGS "/std:c++20")
    else()
      set(CMAKE_REQUIRED_FLAGS "-std=c++20")
    endif()
    check_cxx_source_compiles("${_ARROW_STD_CHRONO_TEST_SOURCE}" ${out_var})
  endfunction()

  if("${ARROW_USE_STD_CHRONO}" STREQUAL "AUTO")
    if(WIN32)
      _arrow_check_std_chrono_support(ARROW_HAVE_STD_CHRONO)
      if(ARROW_HAVE_STD_CHRONO)
        set(ARROW_USE_STD_CHRONO ON)
      else()
        message(STATUS "C++20 chrono timezone support unavailable,"
                       " using vendored datetime fallback")
        set(ARROW_USE_STD_CHRONO OFF)
      endif()
    else()
      # Non-Windows toolchains keep the vendored fallback until the minimum
      # toolchain prerequisites in GH-51267 are met. Toolchains with validated
      # support can opt into std::chrono with -DARROW_USE_STD_CHRONO=ON.
      set(ARROW_USE_STD_CHRONO OFF)
    endif()
  elseif(ARROW_USE_STD_CHRONO)
    _arrow_check_std_chrono_support(ARROW_HAVE_STD_CHRONO)
    if(NOT ARROW_HAVE_STD_CHRONO)
      message(FATAL_ERROR "ARROW_USE_STD_CHRONO=ON requires working C++20 chrono "
                          "timezone support (__cpp_lib_chrono >= 201907L), which "
                          "the current toolchain does not provide")
    endif()
  endif()

  message(STATUS "Using C++20 std::chrono datetime backend: ${ARROW_USE_STD_CHRONO}")

endif()

unset(_ARROW_STD_CHRONO_TEST_SOURCE)
