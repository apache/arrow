// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <chrono>

// Prefer std::chrono when the standard library provides the C++20 timezone APIs.
// Builds may explicitly set ARROW_USE_STD_CHRONO to 0 (vendored date.h) or 1
// (std::chrono) to override automatic selection.
//
// Share this selection between chrono_internal.h and the vendored timezone sources
// datetime/tz.cpp and datetime/ios.mm. Do not include arrow/vendored/datetime.h here:
// it undefines NOEXCEPT, which is needed to compile datetime/tz.cpp.
//
// On Windows, MSVC's standard library uses the system timezone database, while
// libstdc++ reads tzdata files (using TZDIR).
//
// Select the vendored date backend when the C++20 timezone APIs are unavailable.
// On non-Windows, also select it for older libstdc++ versions because of
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=116110 (fully fixed in GCC 16.2).
// The datestamp distinguishes 16.2 (2026-08-07) from 16.1 and early snapshots.
#ifndef ARROW_USE_STD_CHRONO
#  define ARROW_USE_STD_CHRONO 1
#  if !defined(__cpp_lib_chrono) || __cpp_lib_chrono < 201907L
#    undef ARROW_USE_STD_CHRONO
#    define ARROW_USE_STD_CHRONO 0
#  elif !defined(_WIN32) && defined(__GLIBCXX__) && \
      (!defined(_GLIBCXX_RELEASE) || _GLIBCXX_RELEASE < 16 || __GLIBCXX__ < 20260807)
#    undef ARROW_USE_STD_CHRONO
#    define ARROW_USE_STD_CHRONO 0
#  endif
#endif

// Only the vendored Windows text database supports setting its path via Arrow.
// The non-Windows vendored backend uses USE_OS_TZDB (see datetime/visibility.h).
#if ARROW_USE_STD_CHRONO || !defined(_WIN32)
#  define ARROW_CHRONO_USE_OS_TZDB 1
#else
#  define ARROW_CHRONO_USE_OS_TZDB 0
#endif
