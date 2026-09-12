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

#include "arrow/util/crc32c.h"

#include <cstddef>
#include <cstdint>

namespace arrow {
namespace util {

namespace {

// Castagnoli polynomial, reflected representation.
constexpr uint32_t kCrc32cPolynomialReflected = 0x82F63B78u;

uint32_t ComputeCrc32c(const uint8_t* data, std::size_t length) {
  uint32_t crc = 0xFFFFFFFFu;
  for (std::size_t i = 0; i < length; ++i) {
    crc ^= static_cast<uint32_t>(data[i]);
    for (int bit = 0; bit < 8; ++bit) {
      const uint32_t mask = static_cast<uint32_t>(-(crc & 1u));
      crc = (crc >> 1) ^ (kCrc32cPolynomialReflected & mask);
    }
  }
  return ~crc;
}

}  // namespace

uint32_t crc32c_masked(const void* data, std::size_t length) {
  const auto* bytes = static_cast<const uint8_t*>(data);
  const uint32_t crc = ComputeCrc32c(bytes, length);
  // Masking as defined in the Snappy framing format specification.
  return ((crc >> 15) | (crc << 17)) + 0xa282ead8u;
}

}  // namespace util
}  // namespace arrow
