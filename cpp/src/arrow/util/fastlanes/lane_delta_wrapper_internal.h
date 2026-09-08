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

// ---------------------------------------------------------------------------
// Page-level framing for lane-parallel delta, so the layout can be read through
// a Parquet decoder rather than called as a kernel. A kernel measurement skips
// the page header, the decoder's own dispatch and the level handling, so
// dividing one by a full decoder's rate overstates what a reader would gain.
//
// The framing this adds over the kernel's wire layout is a value count:
//
//   [0, 4)      uint32 value count, little endian
//   [4, ...)    the payload described in lane_delta.h
//
// The count has to live here because the payload's own header does not carry
// one, while decode needs it to locate every section. A page's level count
// cannot stand in for it, since that count includes nulls.
//
// Two properties of the payload are the kernel's and are not fixed here. Its
// packed words and its per-block minimums are stored in host byte order, which
// a format specification would have to pin down before this could be written to
// a file anyone else reads. And it is int32 only, so this wrapper is too.
// ---------------------------------------------------------------------------

#pragma once

#include <cstdint>
#include <cstring>
#include <limits>

#include "arrow/buffer.h"
#include "arrow/result.h"
#include "arrow/status.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/fastlanes/lane_delta.h"

namespace arrow {
namespace util {
namespace fastlanes {

/// \brief Page-level encode and decode for the lane-parallel delta layout
///
/// Only int32 is implemented, because the kernel is int32 only.
template <typename T>
class LaneDeltaWrapper;

template <>
class LaneDeltaWrapper<int32_t> {
 public:
  /// Bytes the value count occupies ahead of the payload.
  static constexpr int64_t kCountSize = sizeof(uint32_t);

  /// \brief Upper bound on the bytes Encode writes for `num_values` values
  static int64_t GetMaxCompressedSize(int32_t num_values) {
    if (num_values <= 0) return kCountSize;
    return kCountSize +
           static_cast<int64_t>(LaneDeltaMaxEncodedSize(static_cast<size_t>(num_values)));
  }

  /// \brief Encode values into one page
  ///
  /// \param[in] values the values to encode
  /// \param[in] num_values how many; zero writes a bare count, which is what an
  ///            all-null page encodes to
  /// \param[out] comp output buffer, at least GetMaxCompressedSize bytes
  /// \param[in,out] comp_size in: bytes available; out: bytes written
  static Status Encode(const int32_t* values, int32_t num_values, uint8_t* comp,
                       int64_t* comp_size) {
    if (num_values < 0) {
      return Status::Invalid("Lane delta encode: negative value count ", num_values);
    }
    if (comp == nullptr || comp_size == nullptr) {
      return Status::Invalid("Lane delta encode: null output");
    }
    const int64_t needed = GetMaxCompressedSize(num_values);
    if (*comp_size < needed) {
      return Status::Invalid("Lane delta encode: buffer holds ", *comp_size,
                             " bytes, needs ", needed);
    }
    StoreCount(comp, static_cast<uint32_t>(num_values));
    if (num_values == 0) {
      *comp_size = kCountSize;
      return Status::OK();
    }
    if (values == nullptr) {
      return Status::Invalid("Lane delta encode: null input for ", num_values, " values");
    }
    const size_t written = LaneDeltaEncode<LaneDeltaOrder::kInterleaved>(
        values, static_cast<size_t>(num_values), comp + kCountSize);
    if (written == 0) {
      return Status::Invalid("Lane delta encode: no block width fits ", num_values,
                             " values");
    }
    *comp_size = kCountSize + static_cast<int64_t>(written);
    return Status::OK();
  }

  /// \brief Read how many values a page holds, without decoding it
  static Result<int32_t> DecodeElementCount(const uint8_t* comp, int64_t comp_size) {
    if (comp == nullptr || comp_size < kCountSize) {
      return Status::Invalid("Lane delta page is ", comp_size,
                             " bytes, too short to hold a value count");
    }
    const uint32_t count = LoadCount(comp);
    if (count > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("Lane delta page declares ", count, " values");
    }
    return static_cast<int32_t>(count);
  }

  /// \brief Decode a whole page
  ///
  /// \param[in] comp the page
  /// \param[in] comp_size its size
  /// \param[in] num_values values to decode, which must equal the page's own count
  /// \param[out] values output, sized for num_values
  ///
  /// The kernel reads the payload as 4-byte words, so a payload that does not
  /// start 4-byte aligned is copied to an aligned buffer first. Page buffers are
  /// normally aligned and the copy does not happen.
  static Status Decode(const uint8_t* comp, int64_t comp_size, int32_t num_values,
                       int32_t* values) {
    ARROW_ASSIGN_OR_RAISE(const int32_t declared, DecodeElementCount(comp, comp_size));
    if (declared != num_values) {
      return Status::Invalid("Lane delta page declares ", declared,
                             " values but the caller asked for ", num_values);
    }
    if (num_values == 0) return Status::OK();
    if (values == nullptr) {
      return Status::Invalid("Lane delta decode: null output");
    }

    const uint8_t* payload = comp + kCountSize;
    const int64_t payload_size = comp_size - kCountSize;
    ARROW_ASSIGN_OR_RAISE(
        const int64_t expected,
        PayloadSize(payload, payload_size, static_cast<size_t>(num_values)));
    if (payload_size < expected) {
      return Status::Invalid("Lane delta page holds ", payload_size,
                             " payload bytes, needs ", expected);
    }

    if (reinterpret_cast<uintptr_t>(payload) % sizeof(uint32_t) == 0) {
      LaneDeltaDecode<LaneDeltaOrder::kInterleaved>(
          payload, static_cast<size_t>(num_values), values);
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto aligned, AllocateBuffer(expected));
    std::memcpy(aligned->mutable_data(), payload, static_cast<size_t>(expected));
    LaneDeltaDecode<LaneDeltaOrder::kInterleaved>(
        aligned->data(), static_cast<size_t>(num_values), values);
    return Status::OK();
  }

 private:
  static void StoreCount(uint8_t* comp, uint32_t count) {
    const uint32_t le = ::arrow::bit_util::ToLittleEndian(count);
    std::memcpy(comp, &le, sizeof(le));
  }

  static uint32_t LoadCount(const uint8_t* comp) {
    uint32_t le = 0;
    std::memcpy(&le, comp, sizeof(le));
    return ::arrow::bit_util::FromLittleEndian(le);
  }

  /// Bytes the payload occupies for `n` values, summed from the block widths the
  /// payload itself carries. Reading those widths needs the header to be present,
  /// so that much is checked first.
  static Result<int64_t> PayloadSize(const uint8_t* payload, int64_t payload_size,
                                     size_t n) {
    const int64_t header = static_cast<int64_t>(LaneDeltaHeaderSize(n));
    if (payload_size < header) {
      return Status::Invalid("Lane delta payload is ", payload_size,
                             " bytes, too short for a ", header, " byte header");
    }
    const size_t nblocks = n / kBlockSize;
    const uint8_t* widths = payload + kLanes * sizeof(uint32_t);
    int64_t packed_words = 0;
    for (size_t b = 0; b < nblocks; ++b) {
      if (widths[b] > 32) {
        return Status::Invalid("Lane delta block ", b, " declares width ",
                               static_cast<int>(widths[b]));
      }
      packed_words += static_cast<int64_t>(widths[b]) * static_cast<int64_t>(kLanes);
    }
    const int64_t tail = static_cast<int64_t>(n % kBlockSize);
    return header + packed_words * static_cast<int64_t>(sizeof(uint32_t)) +
           tail * static_cast<int64_t>(sizeof(int32_t));
  }
};

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
