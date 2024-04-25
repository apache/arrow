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

// Implementation helpers for kernels that need to load/gather data from
// multiple, arbitrary indices.
//
// https://en.wikipedia.org/wiki/Gather/scatter_(vector_addressing)

namespace arrow::internal {
inline namespace gather_internal {

// CRTP [1] base class for Gather that provides a gathering loop in terms of
// Write*() methods that must be implemented by the derived class.
//
// [1] https://en.wikipedia.org/wiki/Curiously_recurring_template_pattern
template <class GatherImpl>
class GatherBaseCRTP {
 public:
  // Output offset is not supported by Gather and idx is supposed to have offset
  // pre-applied. idx_validity parameters on functions can use the offset they
  // carry to read the validity bitmap as bitmaps can't have pre-applied offsets
  // (they might not align to byte boundaries).
  GatherBaseCRTP() = default;
  GatherBaseCRTP(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP(GatherBaseCRTP&&) = delete;
  GatherBaseCRTP& operator=(const GatherBaseCRTP&) = delete;
  GatherBaseCRTP& operator=(GatherBaseCRTP&&) = delete;

 protected:
  ARROW_FORCE_INLINE int64_t ExecuteNoNulls(int64_t idx_length) {
    auto* self = static_cast<GatherImpl*>(this);
    for (int64_t position = 0; position < idx_length; position++) {
      self->WriteValue(position);
    }
    return idx_length;
  }

  // See derived Gather classes below for the meaning of the parameters, pre and
  // post-conditions.
  template <bool kOutputIsZeroInitialized, typename IndexCType>
  ARROW_FORCE_INLINE int64_t ExecuteWithNulls(const ArraySpan& src_validity,
                                              int64_t idx_length, const IndexCType* idx,
                                              const ArraySpan& idx_validity,
                                              uint8_t* out_is_valid) {
    auto* self = static_cast<GatherImpl*>(this);
    OptionalBitBlockCounter indices_bit_counter(idx_validity.buffers[0].data,
                                                idx_validity.offset, idx_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < idx_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (!src_validity.MayHaveNulls()) {
        // Source values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither source values nor index nulls
          bit_util::SetBitsTo(out_is_valid, position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            self->WriteValue(position);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(idx_validity.buffers[0].data != nullptr);
            if (idx_validity.IsValid(position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, position);
              self->WriteValue(position);
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          self->WriteZeroSegment(position, block.length);
          position += block.length;
        }
      } else {
        // Source values may be null, so we must do random access into src_validity
        if (block.popcount == block.length) {
          // Faster path: indices are not null but source values may be
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(src_validity.buffers[0].data != nullptr);
            if (src_validity.IsValid(idx[position])) {
              // value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            ARROW_COMPILER_ASSUME(src_validity.buffers[0].data != nullptr);
            ARROW_COMPILER_ASSUME(idx_validity.buffers[0].data != nullptr);
            if (idx_validity.IsValid(position) && src_validity.IsValid(idx[position])) {
              // index is not null && value is not null
              self->WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else if constexpr (!kOutputIsZeroInitialized) {
              self->WriteZero(position);
            }
            ++position;
          }
        } else {
          if constexpr (!kOutputIsZeroInitialized) {
            self->WriteZeroSegment(position, block.length);
          }
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

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t zero_src_offset,
         int64_t idx_length, const IndexCType* idx, uint8_t* out)
      : src_length_(src_length),
        src_(src),
        idx_length_(idx_length),
        idx_(idx),
        out_(out) {
    assert(zero_src_offset == 0);
    assert(src && idx && out);
  }

  ARROW_FORCE_INLINE int64_t Execute() { return this->ExecuteNoNulls(idx_length_); }

  /// \pre If kOutputIsZeroInitialized, then this->out_ has to be zero initialized.
  /// \pre Bits in out_is_valid have to always be zero initialized.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \post If !kOutputIsZeroInitialized, then positions in this->_out containing null
  ///       elements have 0s written to them. This might be less efficient than
  ///       zero-initializing first and calling this->Execute() afterwards.
  /// \return The number of valid elements in out.
  template <bool kOutputIsZeroInitialized = false>
  ARROW_FORCE_INLINE int64_t Execute(const ArraySpan& src_validity,
                                     const ArraySpan& idx_validity,
                                     uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized>(
        src_validity, idx_length_, idx_, idx_validity, out_is_valid);
  }
};

template <typename IndexCType>
class Gather<1, IndexCType> : public GatherBaseCRTP<Gather<1, IndexCType>> {
 private:
  const int64_t src_length_;  // number of elements of bits bytes in src_ after offset
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

  ARROW_FORCE_INLINE int64_t Execute() { return this->ExecuteNoNulls(idx_length_); }

  /// \pre If kOutputIsZeroInitialized, then this->out_ has to be zero initialized.
  /// \pre Bits in out_is_valid have to always be zero initialized.
  /// \post The bits for the valid elements (and only those) are set in out_is_valid.
  /// \post If !kOutputIsZeroInitialized, then positions in this->_out containing null
  ///       elements have 0s written to them. This might be less efficient than
  ///       zero-initializing first and calling this->Execute() afterwards.
  /// \return The number of valid elements in out.
  template <bool kOutputIsZeroInitialized = false>
  ARROW_FORCE_INLINE int64_t Execute(const ArraySpan& src_validity,
                                     const ArraySpan& idx_validity,
                                     uint8_t* out_is_valid) {
    assert(src_length_ == src_validity.length && src_offset_ == src_validity.offset);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized>(
        src_validity, idx_length_, idx_, idx_validity, out_is_valid);
  }
};

}  // namespace gather_internal
}  // namespace arrow::internal
