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
#include <cstddef>
#include <cstdint>
#include "arrow/array/data.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/macros.h"
#include "arrow/util/validity_internal.h"

// Implementation helpers for kernels that need to load (gather) from,
// or store (scatter) data to, multiple, arbitrary indices.
//
// https://en.wikipedia.org/wiki/Gather/scatter_(vector_addressing)

namespace arrow::internal {
inline namespace scatter_gather_internal {

template <class SrcValidity, class IdxValidity>
static constexpr bool EitherMightHaveNulls =
    SrcValidity::kMayHaveBitmap || IdxValidity::kMayHaveBitmap;

// CRTP [1] base class for Gather that provides a gathering loop in terms of
// Write*() methods that must be implemented by the derived class.
//
// [1] https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
template <class GatherImpl>
class GatherBaseCRTP {
 public:
  GatherBaseCRTP() = default;
  GatherBaseCRTP(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP(GatherBaseCRTP&&) = delete;
  GatherBaseCRTP& operator=(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP& operator=(GatherBaseCRTP&&) = delete;

 protected:
  template <typename IndexCType>
  ARROW_FORCE_INLINE int64_t ExecuteNoNulls(int64_t idx_length, const IndexCType* idx) {
    auto* self = static_cast<GatherImpl*>(this);
    for (int64_t position = 0; position < idx_length; position++) {
      self->WriteValue(position);
    }
    return idx_length;
  }

  /// \pre Bits in out_is_valid are already zeroed out.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \return The number of valid elements in out.
  template <class SrcValidity, class IdxValidity, typename IndexCType>
  ARROW_FORCE_INLINE int64_t ExecuteWithNulls(SrcValidity src_validity,
                                              int64_t idx_length, const IndexCType* idx,
                                              IdxValidity idx_validity,
                                              uint8_t* out_is_valid) {
    auto* self = static_cast<GatherImpl*>(this);
    OptionalBitBlockCounter indices_bit_counter(idx_validity.bitmap, idx_validity.offset,
                                                idx_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < idx_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (SrcValidity::kBitmapTag == BitmapTag::kEmpty || src_validity.null_count == 0) {
        // Values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither values nor index nulls
          bit_util::SetBitsTo(out_is_valid, position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            self->WriteValue(position);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (idx_validity.template IsValid<BitmapTag::kChecked>(position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, position);
              self->WriteValue(position);
            } else {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          self->WriteZeroSegment(position, block.length);
          position += block.length;
        }
      } else {
        // Values have nulls, so we must do random access into the values bitmap
        if (block.popcount == block.length) {
          // Faster path: indices are not null but values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (src_validity.template IsValid<BitmapTag::kChecked>(idx[position])) {
              // value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else {
              self->WriteZero(position);
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            if (idx_validity.template IsValid<BitmapTag::kChecked>(position) &&
                src_validity.template IsValid<BitmapTag::kChecked>(idx[position])) {
              // index is not null && value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          self->WriteZeroSegment(position, block.length);
          position += block.length;
        }
      }
    }
    return valid_count;
  }
};

template <int kValueWidthInBits, typename IndexCType,
          std::enable_if_t<kValueWidthInBits % 8 == 0 || kValueWidthInBits == 1, bool> =
              true>
class Gather : public GatherBaseCRTP<Gather<kValueWidthInBits, IndexCType>> {
 public:
  static constexpr int kValueWidth = kValueWidthInBits / 8;

 private:
  const int64_t src_length_;  // number of elements of kValueWidth bytes in src_
  const uint8_t* src_;
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t idx_length,
         const IndexCType* idx, uint8_t* out)
      : src_length_(src_length),
        src_(src),
        idx_length_(idx_length),
        idx_(idx),
        out_(out) {
    assert(src && idx && out);
  }

  // Output offset is not supported by Gather and idx is supposed to have offset
  // pre-applied. idx_validity parameters on functions can use the offset they
  // carry to read the validity bitmap as bitmaps can't have pre-applied offsets
  // (they might not align to byte boundaries).

  Gather(int64_t src_length, const uint8_t* src, int64_t src_offset, int64_t idx_length,
         const IndexCType* idx, uint8_t* out)
      : Gather(/*src_length=*/src_length,
               /*       src=*/src + src_offset * kValueWidth,
               /*idx_length=*/idx_length,
               /*       idx=*/idx,
               /*       out=*/out) {
    assert(src && idx && out);
  }

  void WriteValue(int64_t position) {
    memcpy(out_ + position * kValueWidth, src_ + idx_[position] * kValueWidth,
           kValueWidth);
  }

  void WriteZero(int64_t position) {
    memset(out_ + position * kValueWidth, 0, kValueWidth);
  }

  void WriteZeroSegment(int64_t position, int64_t length) {
    memset(out_ + position * kValueWidth, 0, kValueWidth * length);
  }

  ARROW_FORCE_INLINE
  int64_t Execute() { return this->ExecuteNoNulls(idx_length_, idx_); }

  /// \pre Bits in out_is_valid are already zeroed out.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \return The number of valid elements in out.
  template <class SrcValidity, class IdxValidity>
  ARROW_FORCE_INLINE
      std::enable_if_t<EitherMightHaveNulls<SrcValidity, IdxValidity>, int64_t>
      Execute(SrcValidity src_validity, IdxValidity idx_validity, uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->ExecuteWithNulls(src_validity, idx_length_, idx_, idx_validity,
                                  out_is_valid);
  }
};

template <typename IndexCType>
class Gather<1, IndexCType> : public GatherBaseCRTP<Gather<1, IndexCType>> {
 private:
  const int64_t src_length_;  // number of elements of kValueWidth bytes in src_
  const uint8_t* src_;        // the boolean array data buffer in bits
  const int64_t src_offset_;  // offset in bits
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;  // output boolean array data buffer in bits

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t src_offset, int64_t idx_length,
         const IndexCType* idx, uint8_t* out)
      : src_length_(src_length),
        src_(src),
        src_offset_(src_offset),
        idx_length_(idx_length),
        idx_(idx),
        out_(out) {
    assert(src && idx && out);
  }

  void WriteValue(int64_t position) {
    bit_util::SetBitTo(out_, position,
                       bit_util::GetBit(src_, src_offset_ + idx_[position]));
  }

  void WriteZero(int64_t position) { bit_util::ClearBit(out_, position); }

  void WriteZeroSegment(int64_t position, int64_t block_length) {
    bit_util::SetBitsTo(out_, position, block_length, false);
  }

  ARROW_FORCE_INLINE
  int64_t Execute() { return this->ExecuteNoNulls(idx_length_, idx_); }

  /// \pre Bits in out_is_valid are already zeroed out.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \return The number of valid elements in out.
  template <class SrcValidity, class IdxValidity>
  ARROW_FORCE_INLINE
      std::enable_if_t<EitherMightHaveNulls<SrcValidity, IdxValidity>, int64_t>
      Execute(SrcValidity src_validity, IdxValidity idx_validity, uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length && src_offset_ == src_validity.offset);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->ExecuteWithNulls(src_validity, idx_length_, idx_, idx_validity,
                                  out_is_valid);
  }
};

}  // namespace scatter_gather_internal
}  // namespace arrow::internal
