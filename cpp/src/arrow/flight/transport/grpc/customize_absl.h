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

#if __has_include(<absl/synchronization/mutex.h>)

// HACK: Workaround absl::Mutex ABI incompatibility by making sure the
// non-debug version of Abseil is included
// (https://github.com/conda-forge/abseil-cpp-feedstock/issues/104,
//  https://github.com/abseil/abseil-cpp/issues/1624)
//
// Abseil picks where Mutex::Dtor lives based on NDEBUG:
//
//   mutex.h:  #if defined(NDEBUG) && !ABSL_HAVE_THREAD_SANITIZER &&
//                 !ABSL_BUILD_DLL   -> inline, emitted into this TU
//   mutex.cc: #if !defined(NDEBUG) || ABSL_HAVE_THREAD_SANITIZER ||
//                 ABSL_BUILD_DLL    -> strong, out-of-line
//
// Defining NDEBUG here makes this TU emit its own inline Mutex::Dtor. Whether
// that helps or hurts depends on how the Abseil we link against was built:
//
//  - Abseil built *with* NDEBUG (the conda case above): it inlined Dtor away
//    and never exported it, so a debug TU that expects the out-of-line symbol
//    gets an undefined reference. Emitting our own copy fixes that.
//
//  - Abseil built *without* NDEBUG (a Debug vcpkg/system build): it has the
//    strong out-of-line Dtor. Emitting our own copy is then a duplicate
//    definition, which a *static* Abseil turns into LNK2005 (GH-49585). With a
//    shared Abseil it is harmless, since the definitions live in separate
//    images.
//
// So restrict the hack to the cases where it cannot produce a duplicate:
// anything that is not MSVC-linking-static-Abseil. On MSVC, Abseil's CMake puts
// ABSL_CONSUME_DLL in the INTERFACE compile definitions of its targets, so its
// presence tells us we are consuming the DLL rather than the static library.
#  if !defined(_MSC_VER) || defined(ABSL_CONSUME_DLL)
#    ifndef NDEBUG
#      define ARROW_NO_NDEBUG
#      define NDEBUG
#    endif
#  endif

#  include <absl/synchronization/mutex.h>

#  ifdef ARROW_NO_NDEBUG
#    undef NDEBUG
#  endif

#endif
