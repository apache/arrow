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

#include <cstdint>

#include "arrow/csv/options.h"
#include "arrow/util/simd.h"

namespace arrow {
namespace csv {
namespace internal {

template <bool Quoting, bool Escaping>
class SpecializedOptions {
 public:
  static constexpr bool quoting = Quoting;
  static constexpr bool escaping = Escaping;
};

//
// Bulk filters for packed character matching.
// These filters allow checking multiple CSV bytes at once for specific
// characters (cell delimiter, line delimiter, escape char, etc.).
//

// Heuristic Bloom filters: 1, 2 or 4 bytes at a time.

class BaseBloomFilter {
 public:
  explicit BaseBloomFilter(const ParseOptions& options) : filter_(MakeFilter(options)) {}

 protected:
  using FilterType = uint64_t;
  // 63 for uint64_t
  static constexpr uint8_t kCharMask = static_cast<uint8_t>((8 * sizeof(FilterType)) - 1);

  FilterType MakeFilter(const ParseOptions& options) {
    FilterType filter = 0;
    auto add_char = [&](char c) { filter |= CharFilter(c); };
    add_char('\n');
    add_char('\r');
    add_char(options.delimiter);
    if (options.escaping) {
      add_char(options.escape_char);
    }
    if (options.quoting) {
      add_char(options.quote_char);
    }
    return filter;
  }

  // A given character value will set/test one bit in the 64-bit filter,
  // whose bit number is taken from low bits of the character value.
  //
  // For example 'b' (ASCII value 98) will set/test bit #34 in the filter.
  // If the bit is set in the filter, the given character *may* be part
  // of the matched characters.  If the bit is unset in the filter,
  // the given character *cannot* be part of the matched characters.
  FilterType CharFilter(uint8_t c) const {
    return static_cast<FilterType>(1) << (c & kCharMask);
  }

  FilterType MatchChar(uint8_t c) const { return CharFilter(c) & filter_; }

  const FilterType filter_;
};

template <typename SpecializedOptions>
class BloomFilter1B : public BaseBloomFilter {
 public:
  using WordType = uint8_t;

  using BaseBloomFilter::BaseBloomFilter;

  bool Matches(uint8_t c) const { return (CharFilter(c) & filter_) != 0; }
};

template <typename SpecializedOptions>
class BloomFilter2B : public BaseBloomFilter {
 public:
  using WordType = uint16_t;

  using BaseBloomFilter::BaseBloomFilter;

  bool Matches(uint16_t w) const {
    return (MatchChar(static_cast<uint8_t>(w >> 8)) |
            MatchChar(static_cast<uint8_t>(w))) != 0;
  }
};

template <typename SpecializedOptions>
class BloomFilter4B : public BaseBloomFilter {
 public:
  using WordType = uint32_t;

  using BaseBloomFilter::BaseBloomFilter;

  bool Matches(uint32_t w) const {
    return (MatchChar(static_cast<uint8_t>(w >> 24)) |
            MatchChar(static_cast<uint8_t>(w >> 16)) |
            MatchChar(static_cast<uint8_t>(w >> 8)) |
            MatchChar(static_cast<uint8_t>(w))) != 0;
  }
};

#if defined(ARROW_HAVE_SSE4_2)

// SSE4.2 filter: 8 bytes at a time, using packed compare instruction

// NOTE: on SVE, could use svmatch[_u8] for similar functionality.

template <typename SpecializedOptions>
class SSE42Filter {
 public:
  using WordType = uint64_t;

  explicit SSE42Filter(const ParseOptions& options) : filter_(MakeFilter(options)) {}

  bool Matches(WordType w) const {
    // Look up every byte in `w` in the SIMD filter.
    return _mm_cmpistrc(_mm_set1_epi64x(w), filter_,
                        _SIDD_UBYTE_OPS | _SIDD_CMP_EQUAL_ANY);
  }

 protected:
  using BulkFilterType = __m128i;

  BulkFilterType MakeFilter(const ParseOptions& options) {
    // Make a SIMD word of the characters we want to match
    const char cr = '\r';
    const char lf = '\n';
    const char delim = options.delimiter;
    const char quote = SpecializedOptions::quoting ? options.quote_char : cr;
    const char escape = SpecializedOptions::escaping ? options.escape_char : cr;

    return _mm_set_epi8(delim, quote, escape, lf, cr, cr, cr, cr, cr, cr, cr, cr, cr, cr,
                        cr, cr);
  }

  const BulkFilterType filter_;
};

#elif defined(ARROW_HAVE_NEON)

// NEON filter: 8 bytes at a time, comparing with all special chars.
// We could filter 16 bytes at a time but that actually decreases performance,
// because the filter matches too often on mid-sized cell values.

template <typename SpecializedOptions>
class NeonFilter {
 public:
  // NOTE we cannot use xsimd as it doesn't expose the 64-bit Neon types,
  // only 128-bit.
  using WordType = uint8x8_t;

  explicit NeonFilter(const ParseOptions& options)
      : delim_(vdup_n_u8(options.delimiter)),
        quote_(vdup_n_u8(SpecializedOptions::quoting ? options.quote_char : '\n')),
        escape_(vdup_n_u8(SpecializedOptions::escaping ? options.escape_char : '\n')) {}

  bool Matches(WordType w) const {
    uint8x8_t v;
    v = vceq_u8(w, vdup_n_u8('\r'));
    v = vorr_u8(v, vceq_u8(w, vdup_n_u8('\n')));
    v = vorr_u8(v, vceq_u8(w, delim_));
    if (SpecializedOptions::quoting) {
      v = vorr_u8(v, vceq_u8(w, quote_));
    }
    if (SpecializedOptions::escaping) {
      v = vorr_u8(v, vceq_u8(w, escape_));
    }
    uint64_t r;
    vst1_u64(&r, vreinterpret_u64_u8(v));
    return r != 0;
  }

 private:
  const uint8x8_t delim_, quote_, escape_;
};

#endif

#if defined(ARROW_HAVE_SSE4_2) && (defined(__x86_64__) || defined(_M_X64))
// (the SSE4.2 filter seems to crash on RTools with 32-bit MinGW)
template <typename SpecializedOptions>
using PreferredBulkFilterType = SSE42Filter<SpecializedOptions>;
#elif defined(ARROW_HAVE_NEON)
template <typename SpecializedOptions>
using PreferredBulkFilterType = NeonFilter<SpecializedOptions>;
#else
template <typename SpecializedOptions>
using PreferredBulkFilterType = BloomFilter4B<SpecializedOptions>;
#endif

}  // namespace internal
}  // namespace csv
}  // namespace arrow
