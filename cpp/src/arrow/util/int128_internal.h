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

#include "arrow/util/config.h"
#include "arrow/util/macros.h"

#ifndef ARROW_USE_NATIVE_INT128
#include <boost/multiprecision/cpp_int.hpp>
#endif

namespace arrow {
namespace internal {

// NOTE: __int128_t and boost::multiprecision::int128_t are not interchangeable.
// For example, __int128_t does not have any member function, and does not have
// operator<<(std::ostream, __int128_t). On the other hand, the behavior of
// boost::multiprecision::int128_t might be surprising with some configs (e.g.,
// static_cast<uint64_t>(boost::multiprecision::uint128_t) might return
// ~uint64_t{0} instead of the lower 64 bits of the input).
// Try to minimize the usage of int128_t and uint128_t.
#ifdef ARROW_USE_NATIVE_INT128
using int128_t = __int128_t;
using uint128_t = __uint128_t;
#else
using boost::multiprecision::int128_t;
using boost::multiprecision::uint128_t;
#endif

}  // namespace internal
}  // namespace arrow
