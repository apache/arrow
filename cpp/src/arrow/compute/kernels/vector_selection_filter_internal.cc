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

#include <bit>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/vector_selection_filter_internal.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/datum.h"
#include "arrow/extension_type.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/logging_internal.h"
#include "arrow/util/ubsan.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::OptionalBitBlockCounter;

namespace compute::internal {

namespace {

using FilterState = OptionsWrapper<FilterOptions>;

int64_t GetBitmapFilterOutputSize(const ArraySpan& filter,
                                  FilterOptions::NullSelectionBehavior null_selection) {
  int64_t output_size = 0;

  if (filter.MayHaveNulls()) {
    const uint8_t* filter_is_valid = filter.buffers[0].data;
    BinaryBitBlockCounter bit_counter(filter.buffers[1].data, filter.offset,
                                      filter_is_valid, filter.offset, filter.length);
    int64_t position = 0;
    if (null_selection == FilterOptions::EMIT_NULL) {
      while (position < filter.length) {
        BitBlockCount block = bit_counter.NextOrNotWord();
        output_size += block.popcount;
        position += block.length;
      }
    } else {
      while (position < filter.length) {
        BitBlockCount block = bit_counter.NextAndWord();
        output_size += block.popcount;
        position += block.length;
      }
    }
  } else {
    // The filter has no nulls, so we can use CountSetBits
    output_size = CountSetBits(filter.buffers[1].data, filter.offset, filter.length);
  }
  return output_size;
}

int64_t GetREEFilterOutputSize(const ArraySpan& filter,
                               FilterOptions::NullSelectionBehavior null_selection) {
  const auto& ree_type = checked_cast<const RunEndEncodedType&>(*filter.type);
  DCHECK_EQ(ree_type.value_type()->id(), Type::BOOL);
  int64_t output_size = 0;
  VisitPlainxREEFilterOutputSegments(
      filter, /*filter_may_have_nulls=*/true, null_selection,
      [&output_size](int64_t, int64_t segment_length, bool) {
        output_size += segment_length;
        return true;
      });
  return output_size;
}

}  // namespace

int64_t GetFilterOutputSize(const ArraySpan& filter,
                            FilterOptions::NullSelectionBehavior null_selection) {
  if (filter.type->id() == Type::BOOL) {
    return GetBitmapFilterOutputSize(filter, null_selection);
  }
  DCHECK_EQ(filter.type->id(), Type::RUN_END_ENCODED);
  return GetREEFilterOutputSize(filter, null_selection);
}

namespace {

// ----------------------------------------------------------------------
// Optimized and streamlined filter for primitive types

// Write `num_bits` (1 <= num_bits <= 64) from the low bits of `bits` into `dst` at
// `bit_offset`, touching only bytes in [bit_offset / 8, BytesForBits(bit_offset +
// num_bits)). Bits at positions >= num_bits in `bits` must be 0.
inline void WriteBits64(uint8_t* dst, int64_t bit_offset, uint64_t bits,
                        int num_bits) noexcept {
  DCHECK_GT(num_bits, 0);
  DCHECK_LE(num_bits, 64);
  const int64_t byte_idx = bit_offset >> 3;
  const int bit_rem = static_cast<int>(bit_offset & 7);
  const int num_bytes = static_cast<int>(bit_util::BytesForBits(bit_rem + num_bits));
  if (bit_rem == 0) {
    const uint64_t le = bit_util::ToLittleEndian(bits);
    std::memcpy(dst + byte_idx, &le, static_cast<size_t>(num_bytes));
  } else {
    const uint8_t low_existing =
        dst[byte_idx] & static_cast<uint8_t>((1U << bit_rem) - 1);
    const uint64_t le = bit_util::ToLittleEndian((bits << bit_rem) | low_existing);
    if (num_bytes <= 8) {
      std::memcpy(dst + byte_idx, &le, static_cast<size_t>(num_bytes));
    } else {
      std::memcpy(dst + byte_idx, &le, 8);
      dst[byte_idx + 8] = static_cast<uint8_t>(bits >> (64 - bit_rem));
    }
  }
}

// Compact bits of `data_word` at positions where `mask` has 1s into the low `pop` bits
// of the returned uint64_t (with upper bits zeroed).
inline uint64_t CompactBitmapWord(uint64_t data_word, uint64_t mask, int pop) noexcept {
  if (mask == ~uint64_t{0}) {
    return data_word;
  }
  if (data_word == ~uint64_t{0}) {
    return (pop == 64) ? ~uint64_t{0} : ((uint64_t{1} << pop) - 1);
  }
  if (data_word == 0) {
    return 0;
  }
  if (pop > 32) {
    uint64_t acc = data_word;
    uint64_t inv = ~mask;
    while (inv != 0) {
      const int bit_pos = 63 - std::countl_zero(inv);
      const uint64_t low_mask = (uint64_t{1} << bit_pos) - 1;
      acc = (acc & low_mask) | ((acc >> 1) & ~low_mask);
      inv ^= (uint64_t{1} << bit_pos);
    }
    return acc;
  }
  uint64_t acc = 0;
  int acc_bits = 0;
  uint64_t rem = mask;
  while (rem != 0) {
    const int tz = std::countr_zero(rem);
    acc |= ((data_word >> tz) & uint64_t{1}) << acc_bits;
    ++acc_bits;
    rem &= rem - 1;
  }
  return acc;
}

/// \brief The Filter implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather the physical C type. This way we only
/// generate one take function for each byte width.
///
/// We use compile-time specialization for two variations:
/// - operating on boolean data (using kIsBoolean = true)
/// - operating on fixed-width data of arbitrary width (using kByteWidth = -1),
///   with the actual width only known at runtime
template <int32_t kByteWidth, bool kIsBoolean = false>
class PrimitiveFilterImpl {
 public:
  PrimitiveFilterImpl(const ArraySpan& values, const ArraySpan& filter,
                      FilterOptions::NullSelectionBehavior null_selection,
                      ArrayData* out_arr)
      : byte_width_(util::FixedWidthInBytes(*values.type)),
        values_is_valid_(values.buffers[0].data),
        // No offset applied for boolean because it's a bitmap
        values_data_(kIsBoolean ? values.buffers[1].data
                                : util::OffsetPointerOfFixedByteWidthValues(values)),
        values_null_count_(values.null_count),
        values_offset_(values.offset),
        values_length_(values.length),
        filter_(filter),
        null_selection_(null_selection) {
    if constexpr (kByteWidth >= 0 && !kIsBoolean) {
      DCHECK_EQ(kByteWidth, byte_width_);
    }

    DCHECK_EQ(out_arr->offset, 0);
    if (out_arr->buffers[0] != nullptr) {
      // May be unallocated if neither filter nor values contain nulls
      out_is_valid_ = out_arr->buffers[0]->mutable_data();
    }
    out_data_ = util::MutableFixedWidthValuesPointer(out_arr);
    out_length_ = out_arr->length;
    out_position_ = 0;
  }

  void ExecREEFilter() {
    if (filter_.child_data[1].null_count == 0 && values_null_count_ == 0) {
      DCHECK(!out_is_valid_);
      // Fastest: no nulls in either filter or values
      return VisitPlainxREEFilterOutputSegments(
          filter_, /*filter_may_have_nulls=*/false, null_selection_,
          [&](int64_t position, int64_t segment_length, bool filter_valid) {
            // Fastest path: all values in range are included and not null
            WriteValueSegment(position, segment_length);
            DCHECK(filter_valid);
            return true;
          });
    }
    if (values_is_valid_) {
      DCHECK(out_is_valid_);
      // Slower path: values can be null, so the validity bitmap should be copied
      return VisitPlainxREEFilterOutputSegments(
          filter_, /*filter_may_have_nulls=*/true, null_selection_,
          [&](int64_t position, int64_t segment_length, bool filter_valid) {
            if (filter_valid) {
              CopyBitmap(values_is_valid_, values_offset_ + position, segment_length,
                         out_is_valid_, out_position_);
              WriteValueSegment(position, segment_length);
            } else {
              bit_util::SetBitsTo(out_is_valid_, out_position_, segment_length, false);
              WriteNullSegment(segment_length);
            }
            return true;
          });
    }
    // Faster path: only write to out_is_valid_ if filter contains nulls and
    // null_selection is EMIT_NULL
    if (out_is_valid_) {
      // Set all to valid, so only if nulls are produced by EMIT_NULL, we need
      // to set out_is_valid[i] to false.
      bit_util::SetBitsTo(out_is_valid_, 0, out_length_, true);
    }
    return VisitPlainxREEFilterOutputSegments(
        filter_, /*filter_may_have_nulls=*/true, null_selection_,
        [&](int64_t position, int64_t segment_length, bool filter_valid) {
          if (filter_valid) {
            WriteValueSegment(position, segment_length);
          } else {
            bit_util::SetBitsTo(out_is_valid_, out_position_, segment_length, false);
            WriteNullSegment(segment_length);
          }
          return true;
        });
  }

  void Exec() {
    if (filter_.type->id() == Type::RUN_END_ENCODED) {
      return ExecREEFilter();
    }
    if (values_length_ == 0 || out_length_ == 0) {
      return;
    }

    const auto* filter_is_valid = filter_.buffers[0].data;
    const auto* filter_data = filter_.buffers[1].data;
    const auto filter_offset = filter_.offset;
    const bool has_filter_nulls = (filter_is_valid != nullptr && filter_.null_count != 0);
    const bool has_data_nulls = (values_is_valid_ != nullptr && values_null_count_ != 0);

    // For very high selectivity (>93.75%) with no nulls in filter or values,
    // VisitSetBitRunsVoid coalesces long contiguous runs across word boundaries into
    // large memcpy calls.
    if (!has_filter_nulls && !has_data_nulls && out_is_valid_ == nullptr &&
        out_length_ > values_length_ - (values_length_ >> 4)) {
      ::arrow::internal::VisitSetBitRunsVoid(
          filter_data, filter_offset, values_length_,
          [&](int64_t position, int64_t length) { WriteValueSegment(position, length); });
      return;
    }

    // If output validity bitmap exists but every emitted element is guaranteed valid,
    // fill it once up front and skip validity tracking inside the hot loop.
    const bool write_out_validity =
        (out_is_valid_ != nullptr) &&
        (has_data_nulls ||
         (has_filter_nulls && null_selection_ == FilterOptions::EMIT_NULL));
    if (out_is_valid_ != nullptr && !write_out_validity) {
      bit_util::SetBitsTo(out_is_valid_, 0, out_length_, true);
    }

    ::arrow::internal::BitmapWordReader<uint64_t> filter_data_reader(
        filter_data, filter_offset, values_length_);
    ::arrow::internal::BitmapWordReader<uint64_t> filter_valid_reader;
    if (has_filter_nulls) {
      filter_valid_reader = ::arrow::internal::BitmapWordReader<uint64_t>(
          filter_is_valid, filter_offset, values_length_);
    }
    ::arrow::internal::BitmapWordReader<uint64_t> data_valid_reader;
    if (has_data_nulls) {
      data_valid_reader = ::arrow::internal::BitmapWordReader<uint64_t>(
          values_is_valid_, values_offset_, values_length_);
    }
    ::arrow::internal::BitmapWordReader<uint64_t> bool_data_reader;
    if constexpr (kIsBoolean) {
      bool_data_reader = ::arrow::internal::BitmapWordReader<uint64_t>(
          values_data_, values_offset_, values_length_);
    }

    int64_t in_position = 0;
    int64_t full_run_start = -1;
    int64_t full_run_length = 0;

    auto flush_full_run = [&]() {
      if (full_run_length > 0) {
        if (write_out_validity) {
          if (has_data_nulls) {
            CopyBitmap(values_is_valid_, values_offset_ + full_run_start, full_run_length,
                       out_is_valid_, out_position_);
          } else {
            bit_util::SetBitsTo(out_is_valid_, out_position_, full_run_length, true);
          }
        }
        WriteValueSegment(full_run_start, full_run_length);
        full_run_length = 0;
      }
    };

    int64_t nwords = filter_data_reader.words();
    while (nwords-- > 0) {
      const uint64_t fd = filter_data_reader.NextWord();
      const uint64_t fv =
          has_filter_nulls ? filter_valid_reader.NextWord() : ~uint64_t{0};
      const uint64_t dv = has_data_nulls ? data_valid_reader.NextWord() : ~uint64_t{0};
      const uint64_t bd = [&]() -> uint64_t {
        if constexpr (kIsBoolean) {
          return bool_data_reader.NextWord();
        } else {
          return 0;
        }
      }();

      if (null_selection_ == FilterOptions::DROP || fv == ~uint64_t{0}) {
        const uint64_t mask = fd & fv;
        if (mask == ~uint64_t{0}) {
          if (full_run_length == 0) {
            full_run_start = in_position;
          }
          full_run_length += 64;
        } else {
          flush_full_run();
          if (mask != 0) {
            const int pop = std::popcount(mask);
            if (write_out_validity) {
              const uint64_t out_valid = CompactBitmapWord(dv, mask, pop);
              WriteBits64(out_is_valid_, out_position_, out_valid, pop);
            }
            CompactBlock64(mask, pop, in_position, bd);
          }
        }
      } else {
        flush_full_run();
        // EMIT_NULL with at least one filter null in this 64-bit block.
        const uint64_t emit_mask = (~fv) | fd;
        if (emit_mask != 0) {
          const int64_t out_start = out_position_;
          uint64_t rem = emit_mask;
          uint64_t out_valid_acc = 0;
          int acc_bits = 0;
          while (rem != 0) {
            const int tz = std::countr_zero(rem);
            if ((fv >> tz) & uint64_t{1}) {
              const bool v = !has_data_nulls || (((dv >> tz) & uint64_t{1}) != 0);
              out_valid_acc |= static_cast<uint64_t>(v) << acc_bits;
              WriteValue(in_position + tz);
            } else {
              WriteNull();
            }
            ++acc_bits;
            rem &= rem - 1;
          }
          if (write_out_validity) {
            WriteBits64(out_is_valid_, out_start, out_valid_acc, acc_bits);
          }
        }
      }
      in_position += 64;
    }
    flush_full_run();

    int trailing_bytes = filter_data_reader.trailing_bytes();
    while (trailing_bytes-- > 0) {
      int valid_bits = 0;
      const uint64_t fd = filter_data_reader.NextTrailingByte(valid_bits);
      int unused_bits = 0;
      const uint64_t fv = has_filter_nulls
                              ? filter_valid_reader.NextTrailingByte(unused_bits)
                              : ((uint64_t{1} << valid_bits) - 1);
      const uint64_t dv = has_data_nulls ? data_valid_reader.NextTrailingByte(unused_bits)
                                         : ((uint64_t{1} << valid_bits) - 1);
      const uint64_t valid_mask = (uint64_t{1} << valid_bits) - 1;

      if (null_selection_ == FilterOptions::DROP || (fv & valid_mask) == valid_mask) {
        const uint64_t mask = fd & fv & valid_mask;
        if (mask != 0) {
          const int64_t out_start = out_position_;
          uint64_t rem = mask;
          uint64_t out_valid_acc = 0;
          int acc_bits = 0;
          while (rem != 0) {
            const int tz = std::countr_zero(rem);
            out_valid_acc |= ((dv >> tz) & uint64_t{1}) << acc_bits;
            ++acc_bits;
            WriteValue(in_position + tz);
            rem &= rem - 1;
          }
          if (write_out_validity) {
            WriteBits64(out_is_valid_, out_start, out_valid_acc, acc_bits);
          }
        }
      } else {
        const uint64_t emit_mask = ((~fv) | fd) & valid_mask;
        if (emit_mask != 0) {
          const int64_t out_start = out_position_;
          uint64_t rem = emit_mask;
          uint64_t out_valid_acc = 0;
          int acc_bits = 0;
          while (rem != 0) {
            const int tz = std::countr_zero(rem);
            if ((fv >> tz) & uint64_t{1}) {
              out_valid_acc |= ((dv >> tz) & uint64_t{1}) << acc_bits;
              WriteValue(in_position + tz);
            } else {
              WriteNull();
            }
            ++acc_bits;
            rem &= rem - 1;
          }
          if (write_out_validity) {
            WriteBits64(out_is_valid_, out_start, out_valid_acc, acc_bits);
          }
        }
      }
      in_position += valid_bits;
    }
  }

  template <typename T>
  void CompactTypedBlock64(uint64_t mask, int pop, int64_t in_pos) noexcept {
    const uint8_t* in_bytes = values_data_ + in_pos * static_cast<int64_t>(sizeof(T));
    uint8_t* out_bytes = out_data_ + out_position_ * static_cast<int64_t>(sizeof(T));
    if (pop <= 8 || out_position_ + 64 > out_length_) {
      uint64_t rem = mask;
      while (rem != 0) {
        const int tz = std::countr_zero(rem);
        const T v = util::SafeLoadAs<T>(in_bytes + tz * static_cast<int64_t>(sizeof(T)));
        util::SafeStore(out_bytes, v);
        out_bytes += sizeof(T);
        rem &= rem - 1;
      }
      out_position_ += pop;
      return;
    }
    // Branchless compaction: safe because out_position_ + 64 <= out_length_, so even at
    // b = 63, dst_idx <= 63 and out_position_ + dst_idx < out_length_.
    uint64_t m = mask;
    int64_t dst_idx = 0;
    for (int b = 0; b < 64; ++b) {
      const T v = util::SafeLoadAs<T>(in_bytes + b * static_cast<int64_t>(sizeof(T)));
      util::SafeStore(out_bytes + dst_idx * static_cast<int64_t>(sizeof(T)), v);
      dst_idx += static_cast<int64_t>(m & 1);
      m >>= 1;
    }
    DCHECK_EQ(dst_idx, pop);
    out_position_ += dst_idx;
  }

  void CompactBlock64(uint64_t mask, int pop, int64_t in_pos, uint64_t bool_word) {
    DCHECK_NE(mask, uint64_t{0});
    DCHECK_NE(mask, ~uint64_t{0});
    if constexpr (kIsBoolean) {
      const uint64_t compacted = CompactBitmapWord(bool_word, mask, pop);
      WriteBits64(out_data_, out_position_, compacted, pop);
      out_position_ += pop;
    } else if constexpr (kByteWidth == 1) {
      CompactTypedBlock64<uint8_t>(mask, pop, in_pos);
    } else if constexpr (kByteWidth == 2) {
      CompactTypedBlock64<uint16_t>(mask, pop, in_pos);
    } else if constexpr (kByteWidth == 4) {
      CompactTypedBlock64<uint32_t>(mask, pop, in_pos);
    } else if constexpr (kByteWidth == 8) {
      CompactTypedBlock64<uint64_t>(mask, pop, in_pos);
    } else if constexpr (kByteWidth == 16) {
      struct Bytes16 {
        uint64_t lo;
        uint64_t hi;
      };
      CompactTypedBlock64<Bytes16>(mask, pop, in_pos);
    } else {
      uint64_t rem = mask;
      while (rem != 0) {
        const int tz = std::countr_zero(rem);
        WriteValue(in_pos + tz);
        rem &= rem - 1;
      }
    }
  }

  // Write the next out_position given the selected in_position for the input
  // data and advance out_position
  void WriteValue(int64_t in_position) {
    if constexpr (kIsBoolean) {
      bit_util::SetBitTo(out_data_, out_position_,
                         bit_util::GetBit(values_data_, values_offset_ + in_position));
    } else {
      memcpy(out_data_ + out_position_ * byte_width(),
             values_data_ + in_position * byte_width(), byte_width());
    }
    ++out_position_;
  }

  void WriteValueSegment(int64_t in_start, int64_t length) {
    if constexpr (kIsBoolean) {
      CopyBitmap(values_data_, values_offset_ + in_start, length, out_data_,
                 out_position_);
    } else {
      memcpy(out_data_ + out_position_ * byte_width(),
             values_data_ + in_start * byte_width(), length * byte_width());
    }
    out_position_ += length;
  }

  void WriteNull() {
    if constexpr (kIsBoolean) {
      // Zero the bit
      bit_util::ClearBit(out_data_, out_position_);
    } else {
      // Zero the memory
      memset(out_data_ + out_position_ * byte_width(), 0, byte_width());
    }
    ++out_position_;
  }

  void WriteNullSegment(int64_t length) {
    if constexpr (kIsBoolean) {
      // Zero the bits
      bit_util::SetBitsTo(out_data_, out_position_, length, false);
    } else {
      // Zero the memory
      memset(out_data_ + out_position_ * byte_width(), 0, length * byte_width());
    }
    out_position_ += length;
  }

  constexpr int64_t byte_width() const {
    if constexpr (kByteWidth >= 0) {
      return kByteWidth;
    } else {
      return byte_width_;
    }
  }

 private:
  int64_t byte_width_;
  const uint8_t* values_is_valid_;
  const uint8_t* values_data_;
  int64_t values_null_count_;
  int64_t values_offset_;
  int64_t values_length_;
  const ArraySpan& filter_;
  FilterOptions::NullSelectionBehavior null_selection_;
  uint8_t* out_is_valid_ = NULLPTR;
  uint8_t* out_data_;
  int64_t out_length_;
  int64_t out_position_;
};

}  // namespace

Status PrimitiveFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& values = batch[0].array;
  const ArraySpan& filter = batch[1].array;
  const bool is_ree_filter = filter.type->id() == Type::RUN_END_ENCODED;
  FilterOptions::NullSelectionBehavior null_selection =
      FilterState::Get(ctx).null_selection_behavior;

  int64_t output_length = GetFilterOutputSize(filter, null_selection);

  ArrayData* out_arr = out->array_data().get();

  const bool filter_null_count_is_zero =
      is_ree_filter ? filter.child_data[1].null_count == 0 : filter.null_count == 0;

  // The output precomputed null count is unknown except in the narrow
  // condition that all the values are non-null and the filter will not cause
  // any new nulls to be created.
  if (values.null_count == 0 &&
      (null_selection == FilterOptions::DROP || filter_null_count_is_zero)) {
    out_arr->null_count = 0;
  } else {
    out_arr->null_count = kUnknownNullCount;
  }

  // When neither the values nor filter is known to have any nulls, we will
  // elect the optimized non-null path where there is no need to populate a
  // validity bitmap.
  const bool allocate_validity = values.null_count != 0 || !filter_null_count_is_zero;

  DCHECK(util::IsFixedWidthLike(values));
  const int64_t bit_width = util::FixedWidthInBits(*values.type);
  RETURN_NOT_OK(util::internal::PreallocateFixedWidthArrayData(
      ctx, output_length, /*source=*/values, allocate_validity, out_arr));

  switch (bit_width) {
    case 1:
      PrimitiveFilterImpl<1, /*kIsBoolean=*/true>(values, filter, null_selection, out_arr)
          .Exec();
      break;
    case 8:
      PrimitiveFilterImpl<1>(values, filter, null_selection, out_arr).Exec();
      break;
    case 16:
      PrimitiveFilterImpl<2>(values, filter, null_selection, out_arr).Exec();
      break;
    case 32:
      PrimitiveFilterImpl<4>(values, filter, null_selection, out_arr).Exec();
      break;
    case 64:
      PrimitiveFilterImpl<8>(values, filter, null_selection, out_arr).Exec();
      break;
    case 128:
      // For INTERVAL_MONTH_DAY_NANO, DECIMAL128
      PrimitiveFilterImpl<16>(values, filter, null_selection, out_arr).Exec();
      break;
    case 256:
      // For DECIMAL256
      PrimitiveFilterImpl<32>(values, filter, null_selection, out_arr).Exec();
      break;
    default:
      // Non-specializing on byte width
      PrimitiveFilterImpl<-1>(values, filter, null_selection, out_arr).Exec();
      break;
  }
  return Status::OK();
}

namespace {

// ----------------------------------------------------------------------
// Optimized filter for base binary types (32-bit and 64-bit)

#define BINARY_FILTER_SETUP_COMMON()                                                    \
  const auto raw_offsets = values.GetValues<offset_type>(1);                            \
  const uint8_t* raw_data = values.buffers[2].data;                                     \
                                                                                        \
  TypedBufferBuilder<offset_type> offset_builder(ctx->memory_pool());                   \
  TypedBufferBuilder<uint8_t> data_builder(ctx->memory_pool());                         \
  RETURN_NOT_OK(offset_builder.Reserve(output_length + 1));                             \
                                                                                        \
  /* Presize the data builder with a rough estimate */                                  \
  if (values.length > 0) {                                                              \
    const double mean_value_length = (raw_offsets[values.length] - raw_offsets[0]) /    \
                                     static_cast<double>(values.length);                \
    RETURN_NOT_OK(                                                                      \
        data_builder.Reserve(static_cast<int64_t>(mean_value_length * output_length))); \
  }                                                                                     \
  int64_t space_available = data_builder.capacity();                                    \
  offset_type offset = 0;

#define APPEND_RAW_DATA(DATA, NBYTES)                                  \
  if (ARROW_PREDICT_FALSE(NBYTES > space_available)) {                 \
    RETURN_NOT_OK(data_builder.Reserve(NBYTES));                       \
    space_available = data_builder.capacity() - data_builder.length(); \
  }                                                                    \
  data_builder.UnsafeAppend(DATA, NBYTES);                             \
  space_available -= NBYTES

#define APPEND_SINGLE_VALUE()                                                       \
  do {                                                                              \
    offset_type val_size = raw_offsets[in_position + 1] - raw_offsets[in_position]; \
    APPEND_RAW_DATA(raw_data + raw_offsets[in_position], val_size);                 \
    offset += val_size;                                                             \
  } while (0)

// Optimized binary filter for the case where neither values nor filter have
// nulls
template <typename ArrowType>
Status BinaryFilterNonNullImpl(KernelContext* ctx, const ArraySpan& values,
                               const ArraySpan& filter, int64_t output_length,
                               FilterOptions::NullSelectionBehavior null_selection,
                               ArrayData* out) {
  using offset_type = typename ArrowType::offset_type;
  const bool is_ree_filter = filter.type->id() == Type::RUN_END_ENCODED;

  BINARY_FILTER_SETUP_COMMON();

  auto emit_segment = [&](int64_t position, int64_t length) {
    // Bulk-append raw data
    const offset_type run_data_bytes =
        (raw_offsets[position + length] - raw_offsets[position]);
    APPEND_RAW_DATA(raw_data + raw_offsets[position], run_data_bytes);
    // Append offsets
    for (int64_t i = 0; i < length; ++i) {
      offset_builder.UnsafeAppend(offset);
      offset += raw_offsets[i + position + 1] - raw_offsets[i + position];
    }
    return Status::OK();
  };
  if (is_ree_filter) {
    Status status;
    VisitPlainxREEFilterOutputSegments(
        filter, /*filter_may_have_nulls=*/false, null_selection,
        [&status, emit_segment = std::move(emit_segment)](
            int64_t position, int64_t segment_length, bool filter_valid) {
          DCHECK(filter_valid);
          status = emit_segment(position, segment_length);
          return status.ok();
        });
    RETURN_NOT_OK(std::move(status));
  } else {
    const auto filter_data = filter.buffers[1].data;
    RETURN_NOT_OK(arrow::internal::VisitSetBitRuns(
        filter_data, filter.offset, filter.length, std::move(emit_segment)));
  }

  offset_builder.UnsafeAppend(offset);
  out->length = output_length;
  RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
  return data_builder.Finish(&out->buffers[2]);
}

template <typename ArrowType>
Status BinaryFilterImpl(KernelContext* ctx, const ArraySpan& values,
                        const ArraySpan& filter, int64_t output_length,
                        FilterOptions::NullSelectionBehavior null_selection,
                        ArrayData* out) {
  using offset_type = typename ArrowType::offset_type;

  const bool is_ree_filter = filter.type->id() == Type::RUN_END_ENCODED;

  BINARY_FILTER_SETUP_COMMON();

  const uint8_t* values_is_valid = values.buffers[0].data;
  const int64_t values_offset = values.offset;

  const int64_t out_offset = out->offset;
  uint8_t* out_is_valid = out->buffers[0]->mutable_data();
  // Zero bits and then only have to set valid values to true
  bit_util::SetBitsTo(out_is_valid, out_offset, output_length, false);

  int64_t in_position = 0;
  int64_t out_position = 0;
  if (is_ree_filter) {
    auto emit_segment = [&](int64_t position, int64_t segment_length, bool filter_valid) {
      in_position = position;
      if (filter_valid) {
        // Filter values are all true and not null
        // Some of the values in the block may be null
        for (int64_t i = 0; i < segment_length; ++i, ++in_position, ++out_position) {
          offset_builder.UnsafeAppend(offset);
          if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
            bit_util::SetBit(out_is_valid, out_offset + out_position);
            APPEND_SINGLE_VALUE();
          }
        }
      } else {
        offset_builder.UnsafeAppend(segment_length, offset);
        out_position += segment_length;
      }
      return Status::OK();
    };
    Status status;
    VisitPlainxREEFilterOutputSegments(
        filter, /*filter_may_have_nulls=*/true, null_selection,
        [&status, emit_segment = std::move(emit_segment)](
            int64_t position, int64_t segment_length, bool filter_valid) {
          status = emit_segment(position, segment_length, filter_valid);
          return status.ok();
        });
    RETURN_NOT_OK(std::move(status));
  } else {
    const auto filter_data = filter.buffers[1].data;
    const uint8_t* filter_is_valid = filter.buffers[0].data;
    const int64_t filter_offset = filter.offset;

    // We use 3 block counters for fast scanning of the filter
    //
    // * values_valid_counter: for values null/not-null
    // * filter_valid_counter: for filter null/not-null
    // * filter_counter: for filter true/false
    OptionalBitBlockCounter values_valid_counter(values_is_valid, values_offset,
                                                 values.length);
    OptionalBitBlockCounter filter_valid_counter(filter_is_valid, filter_offset,
                                                 filter.length);
    BitBlockCounter filter_counter(filter_data, filter_offset, filter.length);

    while (in_position < filter.length) {
      BitBlockCount filter_valid_block = filter_valid_counter.NextWord();
      BitBlockCount values_valid_block = values_valid_counter.NextWord();
      BitBlockCount filter_block = filter_counter.NextWord();
      if (filter_block.NoneSet() && null_selection == FilterOptions::DROP) {
        // For this exceedingly common case in low-selectivity filters we can
        // skip further analysis of the data and move on to the next block.
        in_position += filter_block.length;
      } else if (filter_valid_block.AllSet()) {
        // Simpler path: no filter values are null
        if (filter_block.AllSet()) {
          // Fastest path: filter values are all true and not null
          if (values_valid_block.AllSet()) {
            // The values aren't null either
            bit_util::SetBitsTo(out_is_valid, out_offset + out_position,
                                filter_block.length, true);

            // Bulk-append raw data
            offset_type block_data_bytes =
                (raw_offsets[in_position + filter_block.length] -
                 raw_offsets[in_position]);
            APPEND_RAW_DATA(raw_data + raw_offsets[in_position], block_data_bytes);
            // Append offsets
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              offset_builder.UnsafeAppend(offset);
              offset += raw_offsets[in_position + 1] - raw_offsets[in_position];
            }
            out_position += filter_block.length;
          } else {
            // Some of the values in this block are null
            for (int64_t i = 0; i < filter_block.length;
                 ++i, ++in_position, ++out_position) {
              offset_builder.UnsafeAppend(offset);
              if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
                bit_util::SetBit(out_is_valid, out_offset + out_position);
                APPEND_SINGLE_VALUE();
              }
            }
          }
        } else {  // !filter_block.AllSet()
          // Some of the filter values are false, but all not null
          if (values_valid_block.AllSet()) {
            // All the values are not-null, so we can skip null checking for
            // them
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              if (bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                bit_util::SetBit(out_is_valid, out_offset + out_position++);
                APPEND_SINGLE_VALUE();
              }
            }
          } else {
            // Some of the values in the block are null, so we have to check
            // each one
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              if (bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
                  bit_util::SetBit(out_is_valid, out_offset + out_position);
                  APPEND_SINGLE_VALUE();
                }
                ++out_position;
              }
            }
          }
        }
      } else {  // !filter_valid_block.AllSet()
        // Some of the filter values are null, so we have to handle the DROP
        // versus EMIT_NULL null selection behavior.
        if (null_selection == FilterOptions::DROP) {
          // Filter null values are treated as false.
          if (values_valid_block.AllSet()) {
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              if (bit_util::GetBit(filter_is_valid, filter_offset + in_position) &&
                  bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                bit_util::SetBit(out_is_valid, out_offset + out_position++);
                APPEND_SINGLE_VALUE();
              }
            }
          } else {
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              if (bit_util::GetBit(filter_is_valid, filter_offset + in_position) &&
                  bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
                  bit_util::SetBit(out_is_valid, out_offset + out_position);
                  APPEND_SINGLE_VALUE();
                }
                ++out_position;
              }
            }
          }
        } else {
          // EMIT_NULL

          // Filter null values are appended to output as null whether the
          // value in the corresponding slot is valid or not
          if (values_valid_block.AllSet()) {
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              const bool filter_not_null =
                  bit_util::GetBit(filter_is_valid, filter_offset + in_position);
              if (filter_not_null &&
                  bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                bit_util::SetBit(out_is_valid, out_offset + out_position++);
                APPEND_SINGLE_VALUE();
              } else if (!filter_not_null) {
                offset_builder.UnsafeAppend(offset);
                ++out_position;
              }
            }
          } else {
            for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
              const bool filter_not_null =
                  bit_util::GetBit(filter_is_valid, filter_offset + in_position);
              if (filter_not_null &&
                  bit_util::GetBit(filter_data, filter_offset + in_position)) {
                offset_builder.UnsafeAppend(offset);
                if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
                  bit_util::SetBit(out_is_valid, out_offset + out_position);
                  APPEND_SINGLE_VALUE();
                }
                ++out_position;
              } else if (!filter_not_null) {
                offset_builder.UnsafeAppend(offset);
                ++out_position;
              }
            }
          }
        }
      }
    }
  }
  offset_builder.UnsafeAppend(offset);
  out->length = output_length;
  RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
  return data_builder.Finish(&out->buffers[2]);
}

#undef BINARY_FILTER_SETUP_COMMON
#undef APPEND_RAW_DATA
#undef APPEND_SINGLE_VALUE

Status BinaryFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  FilterOptions::NullSelectionBehavior null_selection =
      FilterState::Get(ctx).null_selection_behavior;

  const ArraySpan& values = batch[0].array;
  const ArraySpan& filter = batch[1].array;
  const bool is_ree_filter = filter.type->id() == Type::RUN_END_ENCODED;
  int64_t output_length = GetFilterOutputSize(filter, null_selection);

  ArrayData* out_arr = out->array_data().get();

  const bool filter_null_count_is_zero =
      is_ree_filter ? filter.child_data[1].null_count == 0 : filter.null_count == 0;

  // The output precomputed null count is unknown except in the narrow
  // condition that all the values are non-null and the filter will not cause
  // any new nulls to be created.
  if (values.null_count == 0 &&
      (null_selection == FilterOptions::DROP || filter_null_count_is_zero)) {
    out_arr->null_count = 0;
  } else {
    out_arr->null_count = kUnknownNullCount;
  }
  Type::type type_id = values.type->id();
  if (values.null_count == 0 && filter_null_count_is_zero) {
    // Faster no-nulls case
    if (is_binary_like(type_id)) {
      RETURN_NOT_OK(BinaryFilterNonNullImpl<BinaryType>(
          ctx, values, filter, output_length, null_selection, out_arr));
    } else if (is_large_binary_like(type_id)) {
      RETURN_NOT_OK(BinaryFilterNonNullImpl<LargeBinaryType>(
          ctx, values, filter, output_length, null_selection, out_arr));
    } else {
      DCHECK(false);
    }
  } else {
    // Output may have nulls
    RETURN_NOT_OK(ctx->AllocateBitmap(output_length).Value(&out_arr->buffers[0]));
    if (is_binary_like(type_id)) {
      RETURN_NOT_OK(BinaryFilterImpl<BinaryType>(ctx, values, filter, output_length,
                                                 null_selection, out_arr));
    } else if (is_large_binary_like(type_id)) {
      RETURN_NOT_OK(BinaryFilterImpl<LargeBinaryType>(ctx, values, filter, output_length,
                                                      null_selection, out_arr));
    } else {
      DCHECK(false);
    }
  }

  return Status::OK();
}

// ----------------------------------------------------------------------
// Null filter

Status NullFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  int64_t output_length =
      GetFilterOutputSize(batch[1].array, FilterState::Get(ctx).null_selection_behavior);
  out->value = std::make_shared<NullArray>(output_length)->data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Dictionary filter

Status DictionaryFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  DictionaryArray dict_values(batch[0].array.ToArrayData());
  Datum result;
  RETURN_NOT_OK(Filter(Datum(dict_values.indices()), batch[1].array.ToArrayData(),
                       FilterState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  DictionaryArray filtered_values(dict_values.type(), result.make_array(),
                                  dict_values.dictionary());
  out->value = filtered_values.data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Extension filter

Status ExtensionFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ExtensionArray ext_values(batch[0].array.ToArrayData());
  Datum result;
  RETURN_NOT_OK(Filter(Datum(ext_values.storage()), batch[1].array.ToArrayData(),
                       FilterState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  ExtensionArray filtered_values(ext_values.type(), result.make_array());
  out->value = filtered_values.data();
  return Status::OK();
}

// Transform filter to selection indices and then use Take.
Status FilterWithTakeExec(const ArrayKernelExec& take_exec, KernelContext* ctx,
                          const ExecSpan& batch, ExecResult* out) {
  std::shared_ptr<ArrayData> indices;
  RETURN_NOT_OK(GetTakeIndices(batch[1].array,
                               FilterState::Get(ctx).null_selection_behavior,
                               ctx->memory_pool())
                    .Value(&indices));
  KernelContext take_ctx(*ctx);
  TakeState state{TakeOptions::NoBoundsCheck()};
  take_ctx.SetState(&state);
  ExecSpan take_batch({batch[0], ArraySpan(*indices)}, batch.length);
  return take_exec(&take_ctx, take_batch, out);
}

// Due to the special treatment with their Take kernels, we filter Struct and SparseUnion
// arrays by transforming filter to selection indices and call Take.
Status StructFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterWithTakeExec(StructTakeExec, ctx, batch, out);
}

Status SparseUnionFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterWithTakeExec(SparseUnionTakeExec, ctx, batch, out);
}

// ----------------------------------------------------------------------
// Implement Filter metafunction

Result<std::shared_ptr<RecordBatch>> FilterRecordBatch(const RecordBatch& batch,
                                                       const Datum& filter,
                                                       const FunctionOptions* options,
                                                       ExecContext* ctx) {
  if (batch.num_rows() != filter.length()) {
    return Status::Invalid("Filter inputs must all be the same length");
  }

  // Fetch filter
  const auto& filter_opts = *static_cast<const FilterOptions*>(options);
  ArrayData filter_array;
  switch (filter.kind()) {
    case Datum::ARRAY:
      filter_array = *filter.array();
      break;
    case Datum::CHUNKED_ARRAY: {
      ARROW_ASSIGN_OR_RAISE(auto combined, Concatenate(filter.chunked_array()->chunks()));
      filter_array = *combined->data();
      break;
    }
    default:
      return Status::TypeError("Filter should be array-like");
  }

  // Convert filter to selection vector/indices and use Take
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> indices,
                        GetTakeIndices(filter_array, filter_opts.null_selection_behavior,
                                       ctx->memory_pool()));
  std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
  for (int i = 0; i < batch.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(Datum out, Take(batch.column(i)->data(), Datum(indices),
                                          TakeOptions::NoBoundsCheck(), ctx));
    columns[i] = out.make_array();
  }
  return RecordBatch::Make(batch.schema(), indices->length, std::move(columns));
}

Result<std::shared_ptr<Table>> FilterTable(const Table& table, const Datum& filter,
                                           const FunctionOptions* options,
                                           ExecContext* ctx) {
  if (table.num_rows() != filter.length()) {
    return Status::Invalid("Filter inputs must all be the same length");
  }
  if (table.num_rows() == 0) {
    return Table::Make(table.schema(), table.columns(), 0);
  }

  // Last input element will be the filter array
  const int num_columns = table.num_columns();
  std::vector<ArrayVector> inputs(num_columns + 1);

  // Fetch table columns
  for (int i = 0; i < num_columns; ++i) {
    inputs[i] = table.column(i)->chunks();
  }
  // Fetch filter
  const auto& filter_opts = *static_cast<const FilterOptions*>(options);
  switch (filter.kind()) {
    case Datum::ARRAY:
      inputs.back().push_back(filter.make_array());
      break;
    case Datum::CHUNKED_ARRAY:
      inputs.back() = filter.chunked_array()->chunks();
      break;
    default:
      return Status::TypeError("Filter should be array-like");
  }

  // Rechunk inputs to allow consistent iteration over their respective chunks
  inputs = arrow::internal::RechunkArraysConsistently(inputs);

  // Instead of filtering each column with the boolean filter
  // (which would be slow if the table has a large number of columns: ARROW-10569),
  // convert each filter chunk to indices, and take() the column.
  const int64_t num_chunks = static_cast<int64_t>(inputs.back().size());
  std::vector<ArrayVector> out_columns(num_columns);
  int64_t out_num_rows = 0;

  for (int64_t i = 0; i < num_chunks; ++i) {
    const ArrayData& filter_chunk = *inputs.back()[i]->data();
    ARROW_ASSIGN_OR_RAISE(
        const auto indices,
        GetTakeIndices(filter_chunk, filter_opts.null_selection_behavior,
                       ctx->memory_pool()));

    if (indices->length > 0) {
      // Take from all input columns
      Datum indices_datum{std::move(indices)};
      for (int col = 0; col < num_columns; ++col) {
        const auto& column_chunk = inputs[col][i];
        ARROW_ASSIGN_OR_RAISE(Datum out, Take(column_chunk, indices_datum,
                                              TakeOptions::NoBoundsCheck(), ctx));
        out_columns[col].push_back(std::move(out).make_array());
      }
      out_num_rows += indices->length;
    }
  }

  ChunkedArrayVector out_chunks(num_columns);
  for (int i = 0; i < num_columns; ++i) {
    out_chunks[i] = std::make_shared<ChunkedArray>(std::move(out_columns[i]),
                                                   table.column(i)->type());
  }
  return Table::Make(table.schema(), std::move(out_chunks), out_num_rows);
}

const FunctionDoc filter_doc(
    "Filter with a boolean selection filter",
    ("The output is populated with values from the input at positions\n"
     "where the selection filter is non-zero.  Nulls in the selection filter\n"
     "are handled based on FilterOptions."),
    {"input", "selection_filter"}, "FilterOptions");

class FilterMetaFunction : public MetaFunction {
 public:
  FilterMetaFunction()
      : MetaFunction("filter", Arity::Binary(), filter_doc, GetDefaultFilterOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    if (args[1].kind() != Datum::ARRAY && args[1].kind() != Datum::CHUNKED_ARRAY) {
      return Status::TypeError("Filter should be array-like");
    }

    const auto& filter_type = *args[1].type();
    const bool filter_is_plain_bool = filter_type.id() == Type::BOOL;
    const bool filter_is_ree_bool =
        filter_type.id() == Type::RUN_END_ENCODED &&
        checked_cast<const arrow::RunEndEncodedType&>(filter_type).value_type()->id() ==
            Type::BOOL;
    if (!filter_is_plain_bool && !filter_is_ree_bool) {
      return Status::NotImplemented("Filter argument must be boolean type");
    }

    if (args[0].kind() == Datum::RECORD_BATCH) {
      ARROW_ASSIGN_OR_RAISE(
          std::shared_ptr<RecordBatch> out_batch,
          FilterRecordBatch(*args[0].record_batch(), args[1], options, ctx));
      return Datum(out_batch);
    } else if (args[0].kind() == Datum::TABLE) {
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Table> out_table,
                            FilterTable(*args[0].table(), args[1], options, ctx));
      return Datum(out_table);
    } else {
      return CallFunction("array_filter", args, options, ctx);
    }
  }
};

// ----------------------------------------------------------------------

}  // namespace

const FilterOptions* GetDefaultFilterOptions() {
  static const auto kDefaultFilterOptions = FilterOptions::Defaults();
  return &kDefaultFilterOptions;
}

std::unique_ptr<Function> MakeFilterMetaFunction() {
  return std::make_unique<FilterMetaFunction>();
}

void PopulateFilterKernels(std::vector<SelectionKernelData>* out) {
  auto plain_filter = InputType(Type::BOOL);
  auto ree_filter = InputType(match::RunEndEncoded(Type::BOOL));

  *out = {
      // * x Boolean
      {InputType(match::Primitive()), plain_filter, PrimitiveFilterExec},
      {InputType(match::BinaryLike()), plain_filter, BinaryFilterExec},
      {InputType(match::LargeBinaryLike()), plain_filter, BinaryFilterExec},
      {InputType(null()), plain_filter, NullFilterExec},
      {InputType(Type::FIXED_SIZE_BINARY), plain_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL32), plain_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL64), plain_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL128), plain_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL256), plain_filter, PrimitiveFilterExec},
      {InputType(Type::DICTIONARY), plain_filter, DictionaryFilterExec},
      {InputType(Type::EXTENSION), plain_filter, ExtensionFilterExec},
      {InputType(Type::LIST), plain_filter, ListFilterExec},
      {InputType(Type::LARGE_LIST), plain_filter, LargeListFilterExec},
      {InputType(Type::LIST_VIEW), plain_filter, ListViewFilterExec},
      {InputType(Type::LARGE_LIST_VIEW), plain_filter, LargeListViewFilterExec},
      {InputType(Type::FIXED_SIZE_LIST), plain_filter, FSLFilterExec},
      {InputType(Type::DENSE_UNION), plain_filter, DenseUnionFilterExec},
      {InputType(Type::SPARSE_UNION), plain_filter, SparseUnionFilterExec},
      {InputType(Type::STRUCT), plain_filter, StructFilterExec},
      {InputType(Type::MAP), plain_filter, MapFilterExec},

      // * x REE(Boolean)
      {InputType(match::Primitive()), ree_filter, PrimitiveFilterExec},
      {InputType(match::BinaryLike()), ree_filter, BinaryFilterExec},
      {InputType(match::LargeBinaryLike()), ree_filter, BinaryFilterExec},
      {InputType(null()), ree_filter, NullFilterExec},
      {InputType(Type::FIXED_SIZE_BINARY), ree_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL32), ree_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL64), ree_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL128), ree_filter, PrimitiveFilterExec},
      {InputType(Type::DECIMAL256), ree_filter, PrimitiveFilterExec},
      {InputType(Type::DICTIONARY), ree_filter, DictionaryFilterExec},
      {InputType(Type::EXTENSION), ree_filter, ExtensionFilterExec},
      {InputType(Type::LIST), ree_filter, ListFilterExec},
      {InputType(Type::LARGE_LIST), ree_filter, LargeListFilterExec},
      {InputType(Type::LIST_VIEW), ree_filter, ListViewFilterExec},
      {InputType(Type::LARGE_LIST_VIEW), ree_filter, LargeListViewFilterExec},
      {InputType(Type::FIXED_SIZE_LIST), ree_filter, FSLFilterExec},
      {InputType(Type::DENSE_UNION), ree_filter, DenseUnionFilterExec},
      {InputType(Type::SPARSE_UNION), ree_filter, SparseUnionFilterExec},
      {InputType(Type::STRUCT), ree_filter, StructFilterExec},
      {InputType(Type::MAP), ree_filter, MapFilterExec},
  };
}

}  // namespace compute::internal

}  // namespace arrow
