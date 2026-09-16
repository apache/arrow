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
#include <format>
#include <iterator>
#include <ostream>
#include <sstream>
#include <string>
#if !defined(__cpp_lib_chrono) || __cpp_lib_chrono < 201907L
#  error \"C++20 chrono timezone support (__cpp_lib_chrono >= 201907L) is unavailable\"
#endif
#if !defined(__cpp_lib_format)
#  error \"C++20 formatting support (__cpp_lib_format) is unavailable\"
#endif
int main() {
  // arrow/util/chrono_internal.h (GH-51267) needs working timezone lookup
  // and <format>; the toolchain must provide both, not just the chrono
  // feature-test macro (e.g. GCC 12 advertises __cpp_lib_chrono but has
  // no <format>). The probe only compiles and links, it never runs, so
  // referencing locate_zone here needs no timezone database on the host.
  const std::chrono::time_zone* tz = std::chrono::locate_zone(\"UTC\");
  std::ostringstream os;
  std::vformat_to(std::ostreambuf_iterator<char>(os), \"{:%Y}\",
                  std::make_format_args(std::chrono::system_clock::now()));
  return tz == nullptr;
}
")

  function(_arrow_check_std_chrono_support out_var)
    # Arrow pins the project-wide standard to C++20 (SetupCxxFlags), so
    # try_compile already compiles the probe with /std:c++20.  Passing the
    # switch a second time through CMAKE_REQUIRED_FLAGS made the probe fail
    # spuriously under CMake 4 + MSVC, which silently downgraded Windows
    # AUTO builds to the vendored backend whose tzdb lookups then fail at
    # runtime.  The compiler output is surfaced on failure so a probe
    # regression is diagnosable from CI directly.
    try_compile(${out_var} SOURCE_FROM_VAR
                "arrow_std_chrono_probe.cxx" _ARROW_STD_CHRONO_TEST_SOURCE
                OUTPUT_VARIABLE _chrono_probe_output)
    if(NOT ${out_var})
      message(STATUS "C++20 chrono probe failed with:\n${_chrono_probe_output}")
    endif()
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
                          "timezone and formatting support (__cpp_lib_chrono >= 201907L "
                          "and __cpp_lib_format), which "
                          "the current toolchain does not provide")
    endif()
  endif()

  message(STATUS "Using C++20 std::chrono datetime backend: ${ARROW_USE_STD_CHRONO}")

endif()

unset(_ARROW_STD_CHRONO_TEST_SOURCE)
