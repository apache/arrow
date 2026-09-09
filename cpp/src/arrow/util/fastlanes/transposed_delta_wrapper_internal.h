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
// Page-level framing for the FastLanes paper's lane assignment, so that layout
// can be read through a Parquet decoder rather than called as a kernel. A kernel
// measurement skips the page header, the decoder's own dispatch and the level
// handling, so dividing one by a full decoder's rate overstates what a reader
// would gain.
//
// This is the framing counterpart of the one in lane_delta_wrapper_internal.h,
// which frames the +32 stride instead. The two differ only in which value a
// lane's stored difference is taken against; the framing added here is the same:
//
//   [0, 4)      uint32 value count, little endian
//   [4, ...)    the payload described in transposed_delta.h
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
#include "arrow/util/fastlanes/transposed_delta.h"

namespace arrow {
namespace util {
namespace fastlanes {

/// \brief Page-level encode and decode for the paper's lane assignment
///
/// Only int32 is implemented, because the kernel is int32 only.
template <typename T>
class TransposedDeltaWrapper;

template <>
class TransposedDeltaWrapper<int32_t> {
 public:
  /// Bytes the value count occupies ahead of the payload.
  static constexpr int64_t kCountSize = sizeof(uint32_t);

  /// The bases are delta-coded against each other, which costs about 0.2 bits
  /// per value against 1 bit for storing them plainly.
  static constexpr TransposedBaseCoding kBases = TransposedBaseCoding::kPacked;

  /// A conforming decoder has to return file order, and doing that in the same
  /// pass as the prefix sums is free.
  static constexpr TransposedRepair kRepair = TransposedRepair::kFused;

  /// \brief Upper bound on the bytes Encode writes for `num_values` values
  static int64_t GetMaxCompressedSize(int32_t num_values) {
    if (num_values <= 0) return kCountSize;
    return kCountSize + static_cast<int64_t>(
                            TransposedMaxEncodedSize(static_cast<size_t>(num_values)));
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
      return Status::Invalid("Transposed delta encode: negative value count ",
                             num_values);
    }
    if (comp == nullptr || comp_size == nullptr) {
      return Status::Invalid("Transposed delta encode: null output");
    }
    const int64_t needed = GetMaxCompressedSize(num_values);
    if (*comp_size < needed) {
      return Status::Invalid("Transposed delta encode: buffer holds ", *comp_size,
                             " bytes, needs ", needed);
    }
    StoreCount(comp, static_cast<uint32_t>(num_values));
    if (num_values == 0) {
      *comp_size = kCountSize;
      return Status::OK();
    }
    if (values == nullptr) {
      return Status::Invalid("Transposed delta encode: null input for ", num_values,
                             " values");
    }
    const size_t written = TransposedDeltaEncode<kBases>(
        values, static_cast<size_t>(num_values), comp + kCountSize);
    if (written == 0) {
      return Status::Invalid("Transposed delta encode: no block width fits ", num_values,
                             " values");
    }
    *comp_size = kCountSize + static_cast<int64_t>(written);
    return Status::OK();
  }

  /// \brief Read how many values a page holds, without decoding it
  static Result<int32_t> DecodeElementCount(const uint8_t* comp, int64_t comp_size) {
    if (comp == nullptr || comp_size < kCountSize) {
      return Status::Invalid("Transposed delta page is ", comp_size,
                             " bytes, too short to hold a value count");
    }
    const uint32_t count = LoadCount(comp);
    if (count > static_cast<uint32_t>(std::numeric_limits<int32_t>::max())) {
      return Status::Invalid("Transposed delta page declares ", count, " values");
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
      return Status::Invalid("Transposed delta page declares ", declared,
                             " values but the caller asked for ", num_values);
    }
    if (num_values == 0) return Status::OK();
    if (values == nullptr) {
      return Status::Invalid("Transposed delta decode: null output");
    }

    const uint8_t* payload = comp + kCountSize;
    const int64_t payload_size = comp_size - kCountSize;
    ARROW_ASSIGN_OR_RAISE(
        const int64_t expected,
        PayloadSize(payload, payload_size, static_cast<size_t>(num_values)));
    if (payload_size < expected) {
      return Status::Invalid("Transposed delta page holds ", payload_size,
                             " payload bytes, needs ", expected);
    }

    if (reinterpret_cast<uintptr_t>(payload) % sizeof(uint32_t) == 0) {
      TransposedDeltaDecode<kBases, kRepair>(payload, static_cast<size_t>(num_values),
                                             values);
      return Status::OK();
    }
    ARROW_ASSIGN_OR_RAISE(auto aligned, AllocateBuffer(expected));
    std::memcpy(aligned->mutable_data(), payload, static_cast<size_t>(expected));
    TransposedDeltaDecode<kBases, kRepair>(aligned->data(),
                                           static_cast<size_t>(num_values), values);
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

  /// Bytes the payload occupies for `n` values.
  ///
  /// Unlike the +32 stride's payload, whose per-block sizes are all listed in a
  /// header, this one carries the width of each block's base stream inside that
  /// block. Walking a block therefore means reading it, so the walk checks the
  /// bytes it is about to read are present before reading them, rather than
  /// summing a header and comparing once.
  static Result<int64_t> PayloadSize(const uint8_t* payload, int64_t payload_size,
                                     size_t n) {
    const int64_t header = static_cast<int64_t>(TransposedFixedHeader(n));
    if (payload_size < header) {
      return Status::Invalid("Transposed delta payload is ", payload_size,
                             " bytes, too short for a ", header, " byte header");
    }
    const size_t nblocks = n / kBlockSize;
    int64_t off = header;
    for (size_t b = 0; b < nblocks; ++b) {
      const uint8_t w = payload[b];
      if (w > 32) {
        return Status::Invalid("Transposed delta block ", b, " declares payload width ",
                               static_cast<int>(w));
      }
      if constexpr (kBases == TransposedBaseCoding::kRaw) {
        off += static_cast<int64_t>(kLanes * sizeof(uint32_t));
      } else {
        // A base width and a base minimum, then the packed base deltas.
        if (off + 1 + static_cast<int64_t>(sizeof(int32_t)) > payload_size) {
          return Status::Invalid("Transposed delta payload is ", payload_size,
                                 " bytes, too short for block ", b, "'s bases");
        }
        const uint8_t base_width = payload[off];
        if (base_width > 32) {
          return Status::Invalid("Transposed delta block ", b, " declares base width ",
                                 static_cast<int>(base_width));
        }
        off += static_cast<int64_t>(TransposedBaseBytes<kBases>(base_width));
      }
      if (off > payload_size) {
        return Status::Invalid("Transposed delta payload is ", payload_size,
                               " bytes, too short for block ", b, "'s bases");
      }
      if (w > 0) {
        // The packed words are 4-byte aligned, which the base stream does not
        // leave them, so the encoder pads to the next boundary.
        off += (4 - (off & 3)) & 3;
        off += static_cast<int64_t>(w) * static_cast<int64_t>(kLanes) *
               static_cast<int64_t>(sizeof(uint32_t));
      }
    }
    const int64_t tail = static_cast<int64_t>(n % kBlockSize);
    return off + tail * static_cast<int64_t>(sizeof(int32_t));
  }
};

}  // namespace fastlanes
}  // namespace util
}  // namespace arrow
