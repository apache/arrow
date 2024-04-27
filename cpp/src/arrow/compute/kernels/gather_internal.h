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
#include <vector>

#include "arrow/array/array_base.h"
#include "arrow/array/data.h"
#include "arrow/chunk_resolver.h"
#include "arrow/chunked_array.h"
#include "arrow/type_fwd.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/macros.h"

// Implementation helpers for kernels that need to load/gather fixed-width
// data from multiple, arbitrary indices.
//
// https://en.wikipedia.org/wiki/Gather/scatter_(vector_addressing)

namespace arrow::internal {

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
  ARROW_DISALLOW_COPY_AND_ASSIGN(GatherBaseCRTP);
  ARROW_DEFAULT_MOVE_AND_ASSIGN(GatherBaseCRTP);

 protected:
  template <typename IndexCType>
  bool IsSrcValid(const ArraySpan& src_validity, const IndexCType* idx,
                  int64_t position) const {
    ARROW_COMPILER_ASSUME(src_validity.buffers[0].data != nullptr);
    return src_validity.IsValid(idx[position]);
  }

  ARROW_FORCE_INLINE int64_t ExecuteNoNulls(int64_t idx_length) {
    auto* self = static_cast<GatherImpl*>(this);
    for (int64_t position = 0; position < idx_length; position++) {
      self->WriteValue(position);
    }
    return idx_length;
  }

  // See derived Gather classes below for the meaning of the parameters, pre and
  // post-conditions.
  //
  // src_validity is not necessarily the source of the values that are being
  // gathered (e.g. the source could be a nested fixed-size list array and the
  // values being gathered are from the innermost buffer), so the ArraySpan is
  // used solely to check for nulls in the source values and nothing else.
  //
  // idx_length is the number of elements in idx and consequently the number of
  // bits that might be written to out_is_valid. Member `Write*()` functions will be
  // called with positions from 0 to idx_length - 1.
  //
  // If `kOutputIsZeroInitialized` is true, then `WriteZero()` or `WriteZeroSegment()`
  // doesn't have to be called for resulting null positions. A position is
  // considered null if either the index or the source value is null at that
  // position.
  template <bool kOutputIsZeroInitialized, typename IndexCType,
            class ValiditySpan = ArraySpan>
  ARROW_FORCE_INLINE int64_t ExecuteWithNulls(const ValiditySpan& src_validity,
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
        // Source values may be null, so we must do random access with IsSrcValid()
        if (block.popcount == block.length) {
          // Faster path: indices are not null but source values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (self->IsSrcValid(src_validity, idx, position)) {
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
            ARROW_COMPILER_ASSUME(idx_validity.buffers[0].data != nullptr);
            if (idx_validity.IsValid(position) &&
                self->IsSrcValid(src_validity, idx, position)) {
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

// A gather primitive for primitive fixed-width types with a integral byte width. If
// `kWithFactor` is true, the actual width is a runtime multiple of `kValueWidthInbits`
// (this can be useful for fixed-size list inputs and other input types with unusual byte
// widths that don't deserve value specialization).
template <int kValueWidthInBits, typename IndexCType, bool kWithFactor>
class Gather : public GatherBaseCRTP<Gather<kValueWidthInBits, IndexCType, kWithFactor>> {
 public:
  static_assert(kValueWidthInBits >= 0 && kValueWidthInBits % 8 == 0);
  static constexpr int kValueWidth = kValueWidthInBits / 8;

 private:
  const int64_t src_length_;  // number of elements of kValueWidth bytes in src_
  const uint8_t* src_;
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;
  int64_t factor_;

 public:
  void WriteValue(int64_t position) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memcpy(out_ + position * scaled_factor, src_ + idx_[position] * scaled_factor,
             scaled_factor);
    } else {
      memcpy(out_ + position * kValueWidth, src_ + idx_[position] * kValueWidth,
             kValueWidth);
    }
  }

  void WriteZero(int64_t position) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, kValueWidth);
    }
  }

  void WriteZeroSegment(int64_t position, int64_t length) {
    if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, length * scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, length * kValueWidth);
    }
  }

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t zero_src_offset,
         int64_t idx_length, const IndexCType* idx, uint8_t* out, int64_t factor)
      : src_length_(src_length),
        src_(src),
        idx_length_(idx_length),
        idx_(idx),
        out_(out),
        factor_(factor) {
    assert(zero_src_offset == 0);
    assert(src && idx && out);
    assert((kWithFactor || factor == 1) &&
           "When kWithFactor is false, the factor is assumed to be 1 at compile time");
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

// A gather primitive for boolean inputs. Unlike its counterpart above,
// this does not support passing a non-trivial factor parameter.
template <typename IndexCType>
class Gather</*kValueWidthInBits=*/1, IndexCType, /*kWithFactor=*/false>
    : public GatherBaseCRTP<Gather<1, IndexCType, false>> {
 private:
  const int64_t src_length_;  // number of elements of bits bytes in src_ after offset
  const uint8_t* src_;        // the boolean array data buffer in bits
  const int64_t src_offset_;  // offset in bits
  const int64_t idx_length_;  // number IndexCType elements in idx_
  const IndexCType* idx_;
  uint8_t* out_;  // output boolean array data buffer in bits

 public:
  Gather(int64_t src_length, const uint8_t* src, int64_t src_offset, int64_t idx_length,
         const IndexCType* idx, uint8_t* out, int64_t factor)
      : src_length_(src_length),
        src_(src),
        src_offset_(src_offset),
        idx_length_(idx_length),
        idx_(idx),
        out_(out) {
    assert(src && idx && out);
    assert(factor == 1 &&
           "factor != 1 is not supported when Gather is used to gather bits/booleans");
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
    assert(src_length_ == src_validity.length);
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized>(
        src_validity, idx_length_, idx_, idx_validity, out_is_valid);
  }
};

template <typename IndexCType>
struct ChunkedValiditySpan {
  const ChunkedArray& chunks_validity;
  const IndexCType* chunk_index_vec;
  const IndexCType* index_in_chunk_vec;
  const bool may_have_nulls;

  ChunkedValiditySpan(const ChunkedArray& chunks_validity,
                      const IndexCType* chunk_index_vec,
                      const IndexCType* index_in_chunk_vec)
      : chunks_validity(chunks_validity),
        chunk_index_vec(chunk_index_vec),
        index_in_chunk_vec(index_in_chunk_vec),
        may_have_nulls(chunks_validity.null_count() > 0) {}

  bool MayHaveNulls() const { return may_have_nulls; }

  bool IsValid(int64_t position) const {
    auto chunk_index = chunk_index_vec[position];
    auto index_in_chunk = index_in_chunk_vec[position];
    return chunks_validity.chunk(static_cast<int>(chunk_index))->IsValid(index_in_chunk);
  }
};

template <int kValueWidthInBits, typename IndexCType, bool kWithFactor>
class GatherFromChunks
    : public GatherBaseCRTP<
          GatherFromChunks<kValueWidthInBits, IndexCType, kWithFactor>> {
 private:
  static_assert(!kWithFactor || kValueWidthInBits == 8,
                "kWithFactor is only supported for kValueWidthInBits == 8");
  static_assert(kValueWidthInBits == 1 || kValueWidthInBits % 8 == 0);
  // kValueWidth should not be used if kValueWidthInBits == 1.
  static constexpr int kValueWidth = kValueWidthInBits / 8;

  // src_residual_bit_offsets_[i] is used to store the bit offset of the first byte (0-7)
  // in src_chunks_[i] iff kValueWidthInBits == 1.
  const int* src_residual_bit_offsets_ = NULLPTR;
  // Pre-computed pointers to the start of the values in each chunk.
  const uint8_t* const* src_chunks_;
  // Number indices resolved in chunk_index_vec_/index_in_chunk_vec_
  const int64_t idx_length_;
  const IndexCType* chunk_index_vec_;
  const IndexCType* index_in_chunk_vec_;

  uint8_t* out_;
  int64_t factor_;

 public:
  void WriteValue(int64_t position) {
    auto chunk_index = chunk_index_vec_[position];
    auto index_in_chunk = index_in_chunk_vec_[position];
    auto* chunk = src_chunks_[chunk_index];
    if constexpr (kValueWidthInBits == 1) {
      auto src_offset = src_residual_bit_offsets_[chunk_index];
      bit_util::SetBitTo(out_, position,
                         bit_util::GetBit(chunk, src_offset + index_in_chunk));
    } else if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memcpy(out_ + position * scaled_factor, chunk + index_in_chunk * scaled_factor,
             scaled_factor);
    } else {
      memcpy(out_ + position * kValueWidth, chunk + index_in_chunk * kValueWidth,
             kValueWidth);
    }
  }

  void WriteZero(int64_t position) {
    if constexpr (kValueWidthInBits == 1) {
      bit_util::ClearBit(out_, position);
    } else if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, kValueWidth);
    }
  }

  void WriteZeroSegment(int64_t position, int64_t block_length) {
    if constexpr (kValueWidthInBits == 1) {
      bit_util::SetBitsTo(out_, position, block_length, false);
    } else if constexpr (kWithFactor) {
      const int64_t scaled_factor = kValueWidth * factor_;
      memset(out_ + position * scaled_factor, 0, block_length * scaled_factor);
    } else {
      memset(out_ + position * kValueWidth, 0, block_length * kValueWidth);
    }
  }

  bool IsSrcValid(const ChunkedValiditySpan<IndexCType>& src_validity,
                  const IndexCType* idx, int64_t position) const {
    return src_validity.IsValid(position);
  }

 public:
  GatherFromChunks(const int* src_residual_bit_offsets, const uint8_t* const* src_chunks,
                   const int64_t idx_length, const IndexCType* chunk_index_vec,
                   const IndexCType* index_in_chunk_vec, uint8_t* out, int64_t factor = 1)
      : src_residual_bit_offsets_(src_residual_bit_offsets),
        src_chunks_(src_chunks),
        idx_length_(idx_length),
        chunk_index_vec_(chunk_index_vec),
        index_in_chunk_vec_(index_in_chunk_vec),
        out_(out),
        factor_(factor) {
    assert(src_chunks && chunk_index_vec && index_in_chunk_vec && out);
    if constexpr (kValueWidthInBits == 1) {
      assert(src_residual_bit_offsets);
    }
    assert((kWithFactor || factor == 1) &&
           "When kWithFactor is false, the factor is assumed to be 1 at compile time");
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
  ARROW_FORCE_INLINE int64_t Execute(const ChunkedArray& src_validity,
                                     const ArraySpan& idx_validity,
                                     uint8_t* out_is_valid) {
    assert(idx_length_ == idx_validity.length);
    assert(out_is_valid);
    assert(idx_validity.type->byte_width() == sizeof(IndexCType));
    ChunkedValiditySpan src_validity_span{src_validity, chunk_index_vec_,
                                          index_in_chunk_vec_};
    assert(src_validity_span.MayHaveNulls() || idx_validity.MayHaveNulls());
    // idx=NULLPTR because when it's passed to IsSrcValid() defined above, it's not used.
    return this->template ExecuteWithNulls<kOutputIsZeroInitialized, IndexCType>(
        src_validity_span, idx_length_, /*idx=*/NULLPTR, idx_validity, out_is_valid);
  }
};

}  // namespace arrow::internal
