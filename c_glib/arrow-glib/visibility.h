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

#if (defined(_WIN32) || defined(__CYGWIN__)) && defined(_MSVC_LANG) &&                   \
  !defined(GARROW_STATIC_COMPILATION)
#  define GARROW_EXPORT __declspec(dllexport)
#  define GARROW_IMPORT __declspec(dllimport)
#else
#  define GARROW_EXPORT
#  define GARROW_IMPORT
#endif

#ifdef GARROW_COMPILATION
#  define GARROW_API GARROW_EXPORT
#else
#  define GARROW_API GARROW_IMPORT
#endif

#define GARROW_EXTERN GARROW_API extern
