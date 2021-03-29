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

#include <cassert>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>

#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
#include <xsimd/xsimd.hpp>
#endif

#include "arrow/type_fwd.h"
#include "arrow/util/macros.h"
#include "arrow/util/simd.h"
#include "arrow/util/string_view.h"
#include "arrow/util/ubsan.h"
#include "arrow/util/visibility.h"

namespace arrow {
namespace util {

// Convert a UTF8 string to a wstring (either UTF16 or UTF32, depending
// on the wchar_t width).
ARROW_EXPORT Result<std::wstring> UTF8ToWideString(const std::string& source);

// Similarly, convert a wstring to a UTF8 string.
ARROW_EXPORT Result<std::string> WideStringToUTF8(const std::wstring& source);

namespace internal {

// Copyright (c) 2008-2010 Bjoern Hoehrmann <bjoern@hoehrmann.de>
// See http://bjoern.hoehrmann.de/utf-8/decoder/dfa/ for details.

// A compact state table allowing UTF8 decoding using two dependent
// lookups per byte.  The first lookup determines the character class
// and the second lookup reads the next state.
// In this table states are multiples of 12.
ARROW_EXPORT extern const uint8_t utf8_small_table[256 + 9 * 12];

// Success / reject states when looked up in the small table
static constexpr uint8_t kUTF8DecodeAccept = 0;
static constexpr uint8_t kUTF8DecodeReject = 12;

// An expanded state table allowing transitions using a single lookup
// at the expense of a larger memory footprint (but on non-random data,
// not all the table will end up accessed and cached).
// In this table states are multiples of 256.
ARROW_EXPORT extern uint16_t utf8_large_table[9 * 256];

// Success / reject states when looked up in the large table
static constexpr uint16_t kUTF8ValidateAccept = 0;
static constexpr uint16_t kUTF8ValidateReject = 256;

static inline uint8_t DecodeOneUTF8Byte(uint8_t byte, uint8_t state, uint32_t* codep) {
  uint8_t type = utf8_small_table[byte];

  *codep = (state != kUTF8DecodeAccept) ? (byte & 0x3fu) | (*codep << 6)
                                        : (0xff >> type) & (byte);

  state = utf8_small_table[256 + state + type];
  return state;
}

static inline uint16_t ValidateOneUTF8Byte(uint8_t byte, uint16_t state) {
  return utf8_large_table[state + byte];
}

ARROW_EXPORT void CheckUTF8Initialized();

}  // namespace internal

// This function needs to be called before doing UTF8 validation.
ARROW_EXPORT void InitializeUTF8();

inline bool ValidateUTF8(const uint8_t* data, int64_t size) {
  static constexpr uint64_t high_bits_64 = 0x8080808080808080ULL;
  static constexpr uint32_t high_bits_32 = 0x80808080UL;
  static constexpr uint16_t high_bits_16 = 0x8080U;
  static constexpr uint8_t high_bits_8 = 0x80U;

#ifndef NDEBUG
  internal::CheckUTF8Initialized();
#endif

  while (size >= 8) {
    // XXX This is doing an unaligned access.  Contemporary architectures
    // (x86-64, AArch64, PPC64) support it natively and often have good
    // performance nevertheless.
    uint64_t mask64 = SafeLoadAs<uint64_t>(data);
    if (ARROW_PREDICT_TRUE((mask64 & high_bits_64) == 0)) {
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
    uint16_t state = internal::kUTF8ValidateAccept;
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
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 5
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 6
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // Byte 7
    state = internal::ValidateOneUTF8Byte(*data++, state);
    --size;
    if (state == internal::kUTF8ValidateAccept) {
      continue;  // Got full char, switch back to ASCII detection
    }
    // kUTF8ValidateAccept not reached along 4 transitions has to mean a rejection
    assert(state == internal::kUTF8ValidateReject);
    return false;
  }

  // Check if string tail is full ASCII (common case, fast)
  if (size >= 4) {
    uint32_t tail_mask = SafeLoadAs<uint32_t>(data + size - 4);
    uint32_t head_mask = SafeLoadAs<uint32_t>(data);
    if (ARROW_PREDICT_TRUE(((head_mask | tail_mask) & high_bits_32) == 0)) {
      return true;
    }
  } else if (size >= 2) {
    uint16_t tail_mask = SafeLoadAs<uint16_t>(data + size - 2);
    uint16_t head_mask = SafeLoadAs<uint16_t>(data);
    if (ARROW_PREDICT_TRUE(((head_mask | tail_mask) & high_bits_16) == 0)) {
      return true;
    }
  } else if (size == 1) {
    if (ARROW_PREDICT_TRUE((*data & high_bits_8) == 0)) {
      return true;
    }
  } else {
    /* size == 0 */
    return true;
  }

  // Fall back to UTF8 validation of tail string.
  // Note the state table is designed so that, once in the reject state,
  // we remain in that state until the end.  So we needn't check for
  // rejection at each char (we don't gain much by short-circuiting here).
  uint16_t state = internal::kUTF8ValidateAccept;
  switch (size) {
    case 7:
      state = internal::ValidateOneUTF8Byte(data[size - 7], state);
    case 6:
      state = internal::ValidateOneUTF8Byte(data[size - 6], state);
    case 5:
      state = internal::ValidateOneUTF8Byte(data[size - 5], state);
    case 4:
      state = internal::ValidateOneUTF8Byte(data[size - 4], state);
    case 3:
      state = internal::ValidateOneUTF8Byte(data[size - 3], state);
    case 2:
      state = internal::ValidateOneUTF8Byte(data[size - 2], state);
    case 1:
      state = internal::ValidateOneUTF8Byte(data[size - 1], state);
    default:
      break;
  }
  return ARROW_PREDICT_TRUE(state == internal::kUTF8ValidateAccept);
}

inline bool ValidateUTF8(const util::string_view& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  const size_t length = str.size();

  return ValidateUTF8(data, length);
}

inline bool ValidateAsciiSw(const uint8_t* data, int64_t len) {
  uint8_t orall = 0;

  if (len >= 16) {
    uint64_t or1 = 0, or2 = 0;
    const uint8_t* data2 = data + 8;

    do {
      or1 |= *(const uint64_t*)data;
      or2 |= *(const uint64_t*)data2;
      data += 16;
      data2 += 16;
      len -= 16;
    } while (len >= 16);

    orall = !((or1 | or2) & 0x8080808080808080ULL) - 1;
  }

  while (len--) {
    orall |= *data++;
  }

  if (orall < 0x80) {
    return true;
  } else {
    return false;
  }
}

#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
inline bool ValidateAsciiSimd(const uint8_t* data, int64_t len) {
  using simd_batch = xsimd::batch<int8_t, 16>;

  if (len >= 32) {
    const simd_batch zero(static_cast<int8_t>(0));
    const uint8_t* data2 = data + 16;
    simd_batch or1 = zero, or2 = zero;

    while (len >= 32) {
      or1 |= simd_batch(reinterpret_cast<const int8_t*>(data), xsimd::unaligned_mode{});
      or2 |= simd_batch(reinterpret_cast<const int8_t*>(data2), xsimd::unaligned_mode{});
      data += 32;
      data2 += 32;
      len -= 32;
    }

    // To test for upper bit in all bytes, test whether any of them is negative
    or1 |= or2;
    if (xsimd::any(or1 < zero)) {
      return false;
    }
  }

  return ValidateAsciiSw(data, len);
}
#endif  // ARROW_HAVE_SSE4_2

inline bool ValidateAscii(const uint8_t* data, int64_t len) {
#if defined(ARROW_HAVE_NEON) || defined(ARROW_HAVE_SSE4_2)
  return ValidateAsciiSimd(data, len);
#else
  return ValidateAsciiSw(data, len);
#endif
}

inline bool ValidateAscii(const util::string_view& str) {
  const uint8_t* data = reinterpret_cast<const uint8_t*>(str.data());
  const size_t length = str.size();

  return ValidateAscii(data, length);
}

// Skip UTF8 byte order mark, if any.
ARROW_EXPORT
Result<const uint8_t*> SkipUTF8BOM(const uint8_t* data, int64_t size);

static constexpr uint32_t kMaxUnicodeCodepoint = 0x110000;

static inline bool Utf8IsContinuation(const uint8_t codeunit) {
  return (codeunit & 0xC0) == 0x80;  // upper two bits should be 10
}

static inline bool Utf8Is2ByteStart(const uint8_t codeunit) {
  return (codeunit & 0xE0) == 0xC0;  // upper three bits should be 110
}

static inline bool Utf8Is3ByteStart(const uint8_t codeunit) {
  return (codeunit & 0xF0) == 0xE0;  // upper four bits should be 1110
}

static inline bool Utf8Is4ByteStart(const uint8_t codeunit) {
  return (codeunit & 0xF8) == 0xF0;  // upper five bits should be 11110
}

static inline uint8_t* UTF8Encode(uint8_t* str, uint32_t codepoint) {
  if (codepoint < 0x80) {
    *str++ = codepoint;
  } else if (codepoint < 0x800) {
    *str++ = 0xC0 + (codepoint >> 6);
    *str++ = 0x80 + (codepoint & 0x3F);
  } else if (codepoint < 0x10000) {
    *str++ = 0xE0 + (codepoint >> 12);
    *str++ = 0x80 + ((codepoint >> 6) & 0x3F);
    *str++ = 0x80 + (codepoint & 0x3F);
  } else {
    // Assume proper codepoints are always passed
    assert(codepoint < kMaxUnicodeCodepoint);
    *str++ = 0xF0 + (codepoint >> 18);
    *str++ = 0x80 + ((codepoint >> 12) & 0x3F);
    *str++ = 0x80 + ((codepoint >> 6) & 0x3F);
    *str++ = 0x80 + (codepoint & 0x3F);
  }
  return str;
}

static inline bool UTF8Decode(const uint8_t** data, uint32_t* codepoint) {
  const uint8_t* str = *data;
  if (*str < 0x80) {  // ascii
    *codepoint = *str++;
  } else if (ARROW_PREDICT_FALSE(*str < 0xC0)) {  // invalid non-ascii char
    return false;
  } else if (*str < 0xE0) {
    uint8_t code_unit_1 = (*str++) & 0x1F;  // take last 5 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    *codepoint = (code_unit_1 << 6) + code_unit_2;
  } else if (*str < 0xF0) {
    uint8_t code_unit_1 = (*str++) & 0x0F;  // take last 4 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_3 = (*str++) & 0x3F;  // take last 6 bits
    *codepoint = (code_unit_1 << 12) + (code_unit_2 << 6) + code_unit_3;
  } else if (*str < 0xF8) {
    uint8_t code_unit_1 = (*str++) & 0x07;  // take last 3 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_2 = (*str++) & 0x3F;  // take last 6 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_3 = (*str++) & 0x3F;  // take last 6 bits
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_4 = (*str++) & 0x3F;  // take last 6 bits
    *codepoint =
        (code_unit_1 << 18) + (code_unit_2 << 12) + (code_unit_3 << 6) + code_unit_4;
  } else {  // invalid non-ascii char
    return false;
  }
  *data = str;
  return true;
}

static inline bool UTF8DecodeReverse(const uint8_t** data, uint32_t* codepoint) {
  const uint8_t* str = *data;
  if (*str < 0x80) {  // ascii
    *codepoint = *str--;
  } else {
    if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
      return false;
    }
    uint8_t code_unit_N = (*str--) & 0x3F;  // take last 6 bits
    if (Utf8Is2ByteStart(*str)) {
      uint8_t code_unit_1 = (*str--) & 0x1F;  // take last 5 bits
      *codepoint = (code_unit_1 << 6) + code_unit_N;
    } else {
      if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
        return false;
      }
      uint8_t code_unit_Nmin1 = (*str--) & 0x3F;  // take last 6 bits
      if (Utf8Is3ByteStart(*str)) {
        uint8_t code_unit_1 = (*str--) & 0x0F;  // take last 4 bits
        *codepoint = (code_unit_1 << 12) + (code_unit_Nmin1 << 6) + code_unit_N;
      } else {
        if (ARROW_PREDICT_FALSE(!Utf8IsContinuation(*str))) {
          return false;
        }
        uint8_t code_unit_Nmin2 = (*str--) & 0x3F;  // take last 6 bits
        if (ARROW_PREDICT_TRUE(Utf8Is4ByteStart(*str))) {
          uint8_t code_unit_1 = (*str--) & 0x07;  // take last 3 bits
          *codepoint = (code_unit_1 << 18) + (code_unit_Nmin2 << 12) +
                       (code_unit_Nmin1 << 6) + code_unit_N;
        } else {
          return false;
        }
      }
    }
  }
  *data = str;
  return true;
}

template <class UnaryOperation>
static inline bool UTF8Transform(const uint8_t* first, const uint8_t* last,
                                 uint8_t** destination, UnaryOperation&& unary_op) {
  const uint8_t* i = first;
  uint8_t* out = *destination;
  while (i < last) {
    uint32_t codepoint = 0;
    if (ARROW_PREDICT_FALSE(!UTF8Decode(&i, &codepoint))) {
      return false;
    }
    out = UTF8Encode(out, unary_op(codepoint));
  }
  *destination = out;
  return true;
}

template <class Predicate>
static inline bool UTF8FindIf(const uint8_t* first, const uint8_t* last,
                              Predicate&& predicate, const uint8_t** position) {
  const uint8_t* i = first;
  while (i < last) {
    uint32_t codepoint = 0;
    const uint8_t* current = i;
    if (ARROW_PREDICT_FALSE(!UTF8Decode(&i, &codepoint))) {
      return false;
    }
    if (predicate(codepoint)) {
      *position = current;
      return true;
    }
  }
  *position = last;
  return true;
}

// Same semantics as std::find_if using reverse iterators with the return value
// having the same semantics as std::reverse_iterator<..>.base()
// A reverse iterator physically points to the next address, e.g.:
// &*reverse_iterator(i) == &*(i + 1)
template <class Predicate>
static inline bool UTF8FindIfReverse(const uint8_t* first, const uint8_t* last,
                                     Predicate&& predicate, const uint8_t** position) {
  // converts to a normal point
  const uint8_t* i = last - 1;
  while (i >= first) {
    uint32_t codepoint = 0;
    const uint8_t* current = i;
    if (ARROW_PREDICT_FALSE(!UTF8DecodeReverse(&i, &codepoint))) {
      return false;
    }
    if (predicate(codepoint)) {
      // converts normal pointer to 'reverse iterator semantics'.
      *position = current + 1;
      return true;
    }
  }
  // similar to how an end pointer point to 1 beyond the last, reverse iterators point
  // to the 'first' pointer to indicate out of range.
  *position = first;
  return true;
}

template <class UnaryFunction>
static inline bool UTF8ForEach(const uint8_t* first, const uint8_t* last,
                               UnaryFunction&& f) {
  const uint8_t* i = first;
  while (i < last) {
    uint32_t codepoint = 0;
    if (ARROW_PREDICT_FALSE(!UTF8Decode(&i, &codepoint))) {
      return false;
    }
    f(codepoint);
  }
  return true;
}

template <class UnaryFunction>
static inline bool UTF8ForEach(const std::string& s, UnaryFunction&& f) {
  return UTF8ForEach(reinterpret_cast<const uint8_t*>(s.data()),
                     reinterpret_cast<const uint8_t*>(s.data() + s.length()),
                     std::forward<UnaryFunction>(f));
}

template <class UnaryPredicate>
static inline bool UTF8AllOf(const uint8_t* first, const uint8_t* last, bool* result,
                             UnaryPredicate&& predicate) {
  const uint8_t* i = first;
  while (i < last) {
    uint32_t codepoint = 0;
    if (ARROW_PREDICT_FALSE(!UTF8Decode(&i, &codepoint))) {
      return false;
    }

    if (!predicate(codepoint)) {
      *result = false;
      return true;
    }
  }
  *result = true;
  return true;
}

}  // namespace util
}  // namespace arrow
