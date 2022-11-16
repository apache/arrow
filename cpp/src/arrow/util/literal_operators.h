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

#include "arrow/scalar.h"
#include "arrow/type_fwd.h"
#include "arrow/util/checked_cast.h"
#include "arrow/util/value_parsing.h"

namespace arrow {
namespace util {
namespace arrow_literals {

inline TimestampScalar TimestampScalarFromString(const char* s, size_t length,
                                                 TimeUnit::type unit) {
  TimestampScalar out{timestamp(unit)};
  out.is_valid = ::arrow::internal::ParseValue(
      ::arrow::internal::checked_cast<const TimestampType&>(*out.type), s, length,
      &out.value);
  return TimestampScalar{std::move(out)};
}

inline TimestampScalar operator""_ts_s(const char* s, size_t length) {
  return TimestampScalarFromString(s, length, TimeUnit::SECOND);
}

inline TimestampScalar operator""_ts_ms(const char* s, size_t length) {
  return TimestampScalarFromString(s, length, TimeUnit::MILLI);
}

inline TimestampScalar operator""_ts_us(const char* s, size_t length) {
  return TimestampScalarFromString(s, length, TimeUnit::MICRO);
}

inline TimestampScalar operator""_ts_ns(const char* s, size_t length) {
  return TimestampScalarFromString(s, length, TimeUnit::NANO);
}

}  // namespace arrow_literals
}  // namespace util
}  // namespace arrow
