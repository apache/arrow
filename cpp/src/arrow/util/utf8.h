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

#ifndef ARROW_UTIL_UTF8_H
#define ARROW_UTIL_UTF8_H

#include <cstdint>
#include <cstring>
#include <memory>

#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

static constexpr uint8_t kUTF8Accept = 0;
static constexpr uint8_t kUTF8Reject = 12;

// clang-format off
static const uint8_t utf8d[] = { // NOLINT
  // The first part of the table maps bytes to character classes that
  // to reduce the size of the transition table and create bitmasks.
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,  // NOLINT
   1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,1,  9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,9,  // NOLINT
   7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,7,  // NOLINT
   8,8,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,2,  // NOLINT
  10,3,3,3,3,3,3,3,3,3,3,3,3,4,3,3, 11,6,6,6,5,8,8,8,8,8,8,8,8,8,8,8,  // NOLINT

  // The second part is a transition table that maps a combination
  // of a state of the automaton and a character class to a state.
  // Character classes are between 0 and 11, states are multiples of 12.
   0,12,24,36,60,96,84,12,12,12,48,72, 12,12,12,12,12,12,12,12,12,12,12,12,  // NOLINT
  12, 0,12,12,12,12,12, 0,12, 0,12,12, 12,24,12,12,12,12,12,24,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,24,12,12,12,12, 12,24,12,12,12,12,12,12,12,24,12,12,  // NOLINT
  12,12,12,12,12,12,12,36,12,36,12,12, 12,36,12,12,12,12,12,36,12,36,12,12,  // NOLINT
  12,36,12,12,12,12,12,12,12,12,12,12,  // NOLINT
};
// clang-format on

static inline uint8_t DecodeOneUTF8Byte(uint8_t byte, uint8_t state, uint32_t* codep) {
  uint8_t type = utf8d[byte];

  *codep =
      (state != kUTF8Accept) ? (byte & 0x3fu) | (*codep << 6) : (0xff >> type) & (byte);

  state = utf8d[256 + state + type];
  return state;
}

static inline uint8_t ValidateOneUTF8Byte(uint8_t byte, uint8_t state) {
  uint32_t codepoint;
  return DecodeOneUTF8Byte(byte, state, &codepoint);
}

}  // namespace internal

inline bool ValidateUTF8(const uint8_t* data, int64_t size) {
  static constexpr uint64_t high_bits_64 = 0x8080808080808080ULL;
  // For some reason, defining this variable outside the loop helps clang
  uint64_t mask;

  while (size >= 8) {
    // XXX This is doing an unaligned access.  Contemporary architectures
    // (x86-64, AArch64, PPC64) support it natively and often have good
    // performance nevertheless.
    memcpy(&mask, data, 8);
    if (ARROW_PREDICT_TRUE((mask & high_bits_64) == 0)) {
      // 8 bytes of pure ASCII, move forward
      size -= 8;
      data += 8;
      continue;
    }
    // Non-ASCII run detected.
    // We process at least 4 bytes, to avoid too many spurious 64-bit reads
    // in case the non-ASCII bytes are at the end of the tested 64-bit word.
    // We also only check for rejection at the end since that state is stable
    // (once in reject state, we always remain in reject state).
    // It is guaranteed that size >= 8 when arriving here, which allows
    // us to avoid size checks.
    uint8_t state = internal::kUTF8Accept;
    // Byte 0
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 1
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 2
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 3
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    // Byte 4
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8Accept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 5
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8Accept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 6
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8Accept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 7
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8Accept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // kUTF8Accept not reached along 4 transitions has to mean a rejection
    assert(state == internal::kUTF8Reject);
    return false;
  }

  // Validate string tail one byte at a time
  // Note the state table is designed so that, once in the reject state,
  // we remain in that state until the end.  So we needn't check for
  // rejection at each char (we don't gain much by short-circuiting here).
  uint8_t state = internal::kUTF8Accept;
  while (size-- > 0) {
    state = internal::ValidateOneUTF8Byte(*data++, state);
  }
  return ARROW_PREDICT_TRUE(state == internal::kUTF8Accept);
}

}  // namespace util
}  // namespace arrow

#endif
