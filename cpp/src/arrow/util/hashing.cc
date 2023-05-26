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

#include "arrow/util/hashing.h"

namespace arrow {
namespace internal {

namespace {

// MurmurHash2, 64-bit versions, by Austin Appleby

uint64_t MurmurHash64A(const void* buffer, int len, uint64_t seed) {
  const uint64_t m = 0xc6a4a7935bd1e995LLU;
  const int r = 47;

  uint64_t h = seed ^ (len * m);

  const auto* data = static_cast<const uint64_t*>(buffer);
  const uint64_t* end = data + (len / 8);

  while (data != end) {
    uint64_t k = *data++;

    k *= m;
    k ^= k >> r;
    k *= m;

    h ^= k;
    h *= m;
  }

  const auto* data2 = reinterpret_cast<const uint8_t*>(data);

  switch (len & 7) {
    case 7:
      h ^= static_cast<uint64_t>(data2[6]) << 48;
    case 6:
      h ^= static_cast<uint64_t>(data2[5]) << 40;
    case 5:
      h ^= static_cast<uint64_t>(data2[4]) << 32;
    case 4:
      h ^= static_cast<uint64_t>(data2[3]) << 24;
    case 3:
      h ^= static_cast<uint64_t>(data2[2]) << 16;
    case 2:
      h ^= static_cast<uint64_t>(data2[1]) << 8;
    case 1:
      h ^= static_cast<uint64_t>(data2[0]);
      h *= m;
  };

  h ^= h >> r;
  h *= m;
  h ^= h >> r;

  return h;
}

}  // namespace

hash_t ComputeBitmapHash(const uint8_t* bitmap, int64_t length, int64_t bit_offset,
                         int64_t num_bits) {
  hash_t seed = 0;  // XXX: add seed parameter
  return 0;
}

}  // namespace internal
}  // namespace arrow
