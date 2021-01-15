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

namespace arrow {

template <int bit_width>
struct IntTypes {};

#define IntTypes_DECL(bit_width)               \
  template <>                                  \
  struct IntTypes<bit_width> {                 \
    using signed_type = int##bit_width##_t;    \
    using unsigned_type = uint##bit_width##_t; \
  };

IntTypes_DECL(64);
IntTypes_DECL(32);
IntTypes_DECL(16);

template <uint32_t width>
struct DecimalMeta;

template <>
struct DecimalMeta<16> {
  static constexpr const char* name = "decimal16";
  static constexpr int32_t max_precision = 5;
};

template <>
struct DecimalMeta<32> {
  static constexpr const char* name = "decimal32";
  static constexpr int32_t max_precision = 10;
};

template <>
struct DecimalMeta<64> {
  static constexpr const char* name = "decimal64";
  static constexpr int32_t max_precision = 19;
};

template <>
struct DecimalMeta<128> {
  static constexpr const char* name = "decimal";
  static constexpr int32_t max_precision = 38;
};

template <>
struct DecimalMeta<256> {
  static constexpr const char* name = "decimal256";
  static constexpr int32_t max_precision = 76;
};

}  // namespace arrow
