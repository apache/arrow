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

// Share backend selection with the vendored implementation without including its
// headers. datetime.h undefines macros needed when compiling the implementation.
//
// On Windows, MSVC's standard library uses the system timezone database, while
// libstdc++ reads tzdata files (using TZDIR). Libraries without the C++20 timezone
// APIs, including older libc++, still require the vendored date library.
//
// Use the standard backend by default. Builds may explicitly define
// ARROW_USE_STD_CHRONO to 0 or 1 when they need to select a backend.
//
// Automatically disable the default for libraries without the C++20 timezone APIs.
// On non-Windows, older libstdc++ versions also need the fallback because of
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=116110 (fully fixed in GCC 16.2).
// Check library macros, not __GNUC__, so Clang using libstdc++ agrees with GCC.
// The datestamp distinguishes 16.2 (2026-08-07) from 16.1 and early snapshots.
// Keep the existing Windows backend selection unchanged.
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
