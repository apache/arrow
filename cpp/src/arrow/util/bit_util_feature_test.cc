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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <bit>

#if defined(__cpp_lib_int_pow2) && __cpp_lib_int_pow2 >= 202002L
// Simulate the macOS 12 SDK, which defines int_pow2 but not bitops.
#  undef __apple_build_version__
#  define __apple_build_version__ 1
#  undef __cpp_lib_bitops
#endif

#include "arrow/util/bit_util.h"

#if defined(__cpp_lib_int_pow2) && __cpp_lib_int_pow2 >= 202002L
static_assert(arrow::bit_util::BitWidth(5U) == 3);
static_assert(arrow::bit_util::HasSingleBit(8U));
#endif
