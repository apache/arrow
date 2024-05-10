/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#if defined(_WIN32) || defined(__CYGWIN__)
// Windows

#  if defined(__cplusplus) && defined(__GNUC__) && !defined(__clang__)
// Use C++ attribute syntax where possible to avoid GCC parser bug
// (https://stackoverflow.com/questions/57993818/gcc-how-to-combine-attribute-dllexport-and-nodiscard-in-a-struct-de)
#    define GARROW_DLLEXPORT [[gnu::dllexport]]
#    define GARROW_DLLIMPORT [[gnu::dllimport]]
#  else
#    define GARROW_DLLEXPORT __declspec(dllexport)
#    define GARROW_DLLIMPORT __declspec(dllimport)
#  endif

#  ifdef GARROW_EXPORTING
#    define GARROW_EXPORT GARROW_DLLEXPORT
#  else
#    define GARROW_EXPORT GARROW_DLLIMPORT
#  endif

#else

// Non-Windows

#  if defined(__cplusplus) && (defined(__GNUC__) || defined(__clang__))
#    ifndef GARROW_EXPORT
#      define GARROW_EXPORT [[gnu::visibility("default")]]
#    endif
#  else
// Not C++, or not gcc/clang
#    ifndef GARROW_EXPORT
#      define GARROW_EXPORT
#    endif
#  endif

#endif // Non-Windows
