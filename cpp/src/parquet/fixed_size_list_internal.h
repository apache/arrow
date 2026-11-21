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

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <span>

namespace parquet::internal {

// Non-consuming recognition of the hybrid RLE/bit-packed streams used for a
// single repeated leaf. Unrecognized layouts are handled by the normal decoder.
class FixedSizeListLevelMatcher {
 public:
  static bool AllPresent(std::span<const uint8_t> bytes, int16_t max_level,
                         int32_t count) {
    if (max_level == 0) return bytes.empty();
    uint32_t header;
    if (!ReadHeader(&bytes, &header) || (header & 1) ||
        (header >> 1) != static_cast<uint32_t>(count))
      return false;
    const int value_bytes = max_level <= 255 ? 1 : 2;
    if (bytes.size() != static_cast<size_t>(value_bytes)) return false;
    const int value = bytes[0] | (value_bytes == 2 ? bytes[1] << 8 : 0);
    return value == max_level;
  }

  static bool Repetitions(std::span<const uint8_t> bytes, int32_t count, int32_t width,
                          int32_t phase) {
    if (count <= 0 || width <= 0 || phase < 0 || phase >= width) return false;
    // Cache every small-width phase once. The hot loop compares eight encoded
    // bytes at a time rather than regenerating the expected bits for each word.
    static const auto patterns = [] {
      std::array<std::array<std::array<uint8_t, 8>, 16>, 17> out{};
      for (int w = 1; w <= 16; ++w) {
        for (int p = 0; p < w; ++p) {
          for (int b = 0; b < 8; ++b) {
            out[w][p][b] = ExpectedByte(w, (p + b * 8) % w);
          }
        }
      }
      return out;
    }();
    const auto original = bytes;
    const int32_t initial_phase = phase;
    bool tried_stride = false;
    int64_t remaining = count;
    while (remaining > 0) {
      uint32_t header;
      if (!ReadHeader(&bytes, &header) || header < 2) return false;
      if (!(header & 1)) {
        const int64_t run = header >> 1;
        if (bytes.empty() || run > remaining) return false;
        const uint8_t value = bytes.front();
        bytes = bytes.subspan(1);
        if (value == 0) {
          if (phase != 0 || (width != 1 && run != 1)) return false;
        } else if (value == 1) {
          if (phase == 0 || run > width - phase) return false;
        } else {
          return false;
        }
        phase = static_cast<int32_t>((phase + run) % width);
        remaining -= run;
      } else {
        // One packed group contains eight one-bit repetition levels. Only the
        // final group may contain padding beyond the page's logical count.
        const uint64_t groups = header >> 1;
        if (groups > bytes.size() || groups * 8 > static_cast<uint64_t>(remaining) + 7)
          return false;
        int64_t bits = std::min<int64_t>(remaining, groups * 8);
        const int64_t logical_bits = bits;
        size_t offset = 0;
        // The packed pattern repeats after width/gcd(width,8) bytes. Small
        // widths therefore use word-sized comparisons without expanding levels.
        while (bits >= 64) {
          if (width <= 16) {
            if (std::memcmp(bytes.data() + offset, patterns[width][phase].data(), 8) !=
                0) {
              return false;
            }
            phase = (phase + 64) % width;
            offset += 8;
            bits -= 64;
            continue;
          }
          uint8_t expected[8];
          int32_t next = phase;
          for (auto& byte : expected) {
            byte = ExpectedByte(width, next);
            next = static_cast<int32_t>((static_cast<int64_t>(next) + 8) % width);
          }
          if (std::memcmp(bytes.data() + offset, expected, 8) != 0) return false;
          phase = next;
          offset += 8;
          bits -= 64;
        }
        while (bits > 0) {
          const int take = static_cast<int>(std::min<int64_t>(8, bits));
          const uint8_t mask = static_cast<uint8_t>((1U << take) - 1);
          if ((bytes[offset] & mask) != (ExpectedByte(width, phase) & mask)) {
            return false;
          }
          phase = static_cast<int32_t>((static_cast<int64_t>(phase) + take) % width);
          ++offset;
          bits -= take;
        }
        bytes = bytes.subspan(static_cast<size_t>(groups));
        remaining -= logical_bits;
      }
      // Once a validated encoded prefix returns to its incoming list phase,
      // exact copies describe the same whole number of lists. Compare those
      // strides directly; a different legal run split resumes the run walker.
      if (width > 8 && !tried_stride && phase == initial_phase) {
        tried_stride = true;
        const int64_t period = count - remaining;
        const size_t stride = original.size() - bytes.size();
        while (remaining >= period && bytes.size() >= stride &&
               std::memcmp(bytes.data(), original.data(), stride) == 0) {
          bytes = bytes.subspan(stride);
          remaining -= period;
        }
      }
    }
    return bytes.empty();
  }

 private:
  static uint8_t ExpectedByte(int32_t width, int32_t phase) {
    if (width == 1) return 0;
    uint8_t byte = 0xff;
    int64_t zero = phase == 0 ? 0 : width - phase;
    for (; zero < 8; zero += width) byte &= static_cast<uint8_t>(~(1U << zero));
    return byte;
  }

  static bool ReadHeader(std::span<const uint8_t>* bytes, uint32_t* header) {
    *header = 0;
    for (int shift = 0; shift < 35; shift += 7) {
      if (bytes->empty()) return false;
      const uint8_t byte = bytes->front();
      *bytes = bytes->subspan(1);
      if (shift == 28 && byte > 15) return false;
      *header |= static_cast<uint32_t>(byte & 127) << shift;
      if (!(byte & 128)) return true;
    }
    return false;
  }
};

}  // namespace parquet::internal
