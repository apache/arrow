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

#include <cstdint>
#include <cstring>
#include <memory>
#include <type_traits>
#include <vector>

#include "arrow/array/data.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/exec.h"
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

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::OptionalBitBlockCounter;

namespace compute {
namespace internal {

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

// TODO(pr-35750): Handle run-end encoded filters in compute kernels

}  // namespace

int64_t GetFilterOutputSize(const ArraySpan& filter,
                            FilterOptions::NullSelectionBehavior null_selection) {
  return GetBitmapFilterOutputSize(filter, null_selection);
}

namespace {

// ----------------------------------------------------------------------
// Optimized and streamlined filter for primitive types

// Use either BitBlockCounter or BinaryBitBlockCounter to quickly scan filter a
// word at a time for the DROP selection type.
class DropNullCounter {
 public:
  // validity bitmap may be null
  DropNullCounter(const uint8_t* validity, const uint8_t* data, int64_t offset,
                  int64_t length)
      : data_counter_(data, offset, length),
        data_and_validity_counter_(data, offset, validity, offset, length),
        has_validity_(validity != nullptr) {}

  BitBlockCount NextBlock() {
    if (has_validity_) {
      // filter is true AND not null
      return data_and_validity_counter_.NextAndWord();
    } else {
      return data_counter_.NextWord();
    }
  }

 private:
  // For when just data is present, but no validity bitmap
  BitBlockCounter data_counter_;

  // For when both validity bitmap and data are present
  BinaryBitBlockCounter data_and_validity_counter_;
  const bool has_validity_;
};

/// \brief The Filter implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather the physical C type. This way we only
/// generate one take function for each byte width. We use the same
/// implementation here for boolean and fixed-byte-size inputs with some
/// template specialization.
template <typename ArrowType>
class PrimitiveFilterImpl {
 public:
  using T = typename std::conditional<std::is_same<ArrowType, BooleanType>::value,
                                      uint8_t, typename ArrowType::c_type>::type;

  PrimitiveFilterImpl(const ArraySpan& values, const ArraySpan& filter,
                      FilterOptions::NullSelectionBehavior null_selection,
                      ArrayData* out_arr)
      : values_is_valid_(values.buffers[0].data),
        values_data_(reinterpret_cast<const T*>(values.buffers[1].data)),
        values_null_count_(values.null_count),
        values_offset_(values.offset),
        values_length_(values.length),
        filter_is_valid_(filter.buffers[0].data),
        filter_data_(filter.buffers[1].data),
        filter_null_count_(filter.null_count),
        filter_offset_(filter.offset),
        null_selection_(null_selection) {
    if (values.type->id() != Type::BOOL) {
      // No offset applied for boolean because it's a bitmap
      values_data_ += values.offset;
    }

    if (out_arr->buffers[0] != nullptr) {
      // May not be allocated if neither filter nor values contains nulls
      out_is_valid_ = out_arr->buffers[0]->mutable_data();
    }
    out_data_ = reinterpret_cast<T*>(out_arr->buffers[1]->mutable_data());
    out_offset_ = out_arr->offset;
    out_length_ = out_arr->length;
    out_position_ = 0;
  }

  void ExecNonNull() {
    // Fast filter when values and filter are not null
    ::arrow::internal::VisitSetBitRunsVoid(
        filter_data_, filter_offset_, values_length_,
        [&](int64_t position, int64_t length) { WriteValueSegment(position, length); });
  }

  void Exec() {
    if (filter_null_count_ == 0 && values_null_count_ == 0) {
      return ExecNonNull();
    }

    // Bit counters used for both null_selection behaviors
    DropNullCounter drop_null_counter(filter_is_valid_, filter_data_, filter_offset_,
                                      values_length_);
    OptionalBitBlockCounter data_counter(values_is_valid_, values_offset_,
                                         values_length_);
    OptionalBitBlockCounter filter_valid_counter(filter_is_valid_, filter_offset_,
                                                 values_length_);

    auto WriteNotNull = [&](int64_t index) {
      bit_util::SetBit(out_is_valid_, out_offset_ + out_position_);
      // Increments out_position_
      WriteValue(index);
    };

    auto WriteMaybeNull = [&](int64_t index) {
      bit_util::SetBitTo(out_is_valid_, out_offset_ + out_position_,
                         bit_util::GetBit(values_is_valid_, values_offset_ + index));
      // Increments out_position_
      WriteValue(index);
    };

    int64_t in_position = 0;
    while (in_position < values_length_) {
      BitBlockCount filter_block = drop_null_counter.NextBlock();
      BitBlockCount filter_valid_block = filter_valid_counter.NextWord();
      BitBlockCount data_block = data_counter.NextWord();
      if (filter_block.AllSet() && data_block.AllSet()) {
        // Fastest path: all values in block are included and not null
        bit_util::SetBitsTo(out_is_valid_, out_offset_ + out_position_,
                            filter_block.length, true);
        WriteValueSegment(in_position, filter_block.length);
        in_position += filter_block.length;
      } else if (filter_block.AllSet()) {
        // Faster: all values are selected, but some values are null
        // Batch copy bits from values validity bitmap to output validity bitmap
        CopyBitmap(values_is_valid_, values_offset_ + in_position, filter_block.length,
                   out_is_valid_, out_offset_ + out_position_);
        WriteValueSegment(in_position, filter_block.length);
        in_position += filter_block.length;
      } else if (filter_block.NoneSet() && null_selection_ == FilterOptions::DROP) {
        // For this exceedingly common case in low-selectivity filters we can
        // skip further analysis of the data and move on to the next block.
        in_position += filter_block.length;
      } else {
        // Some filter values are false or null
        if (data_block.AllSet()) {
          // No values are null
          if (filter_valid_block.AllSet()) {
            // Filter is non-null but some values are false
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteNotNull(in_position);
              }
              ++in_position;
            }
          } else if (null_selection_ == FilterOptions::DROP) {
            // If any values are selected, they ARE NOT null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (bit_util::GetBit(filter_is_valid_, filter_offset_ + in_position) &&
                  bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteNotNull(in_position);
              }
              ++in_position;
            }
          } else {  // null_selection == FilterOptions::EMIT_NULL
            // Data values in this block are not null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              const bool is_valid =
                  bit_util::GetBit(filter_is_valid_, filter_offset_ + in_position);
              if (is_valid &&
                  bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                // Filter slot is non-null and set
                WriteNotNull(in_position);
              } else if (!is_valid) {
                // Filter slot is null, so we have a null in the output
                bit_util::ClearBit(out_is_valid_, out_offset_ + out_position_);
                WriteNull();
              }
              ++in_position;
            }
          }
        } else {  // !data_block.AllSet()
          // Some values are null
          if (filter_valid_block.AllSet()) {
            // Filter is non-null but some values are false
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteMaybeNull(in_position);
              }
              ++in_position;
            }
          } else if (null_selection_ == FilterOptions::DROP) {
            // If any values are selected, they ARE NOT null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (bit_util::GetBit(filter_is_valid_, filter_offset_ + in_position) &&
                  bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteMaybeNull(in_position);
              }
              ++in_position;
            }
          } else {  // null_selection == FilterOptions::EMIT_NULL
            // Data values in this block are not null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              const bool is_valid =
                  bit_util::GetBit(filter_is_valid_, filter_offset_ + in_position);
              if (is_valid &&
                  bit_util::GetBit(filter_data_, filter_offset_ + in_position)) {
                // Filter slot is non-null and set
                WriteMaybeNull(in_position);
              } else if (!is_valid) {
                // Filter slot is null, so we have a null in the output
                bit_util::ClearBit(out_is_valid_, out_offset_ + out_position_);
                WriteNull();
              }
              ++in_position;
            }
          }
        }
      }  // !filter_block.AllSet()
    }    // while(in_position < values_length_)
  }

  // Write the next out_position given the selected in_position for the input
  // data and advance out_position
  void WriteValue(int64_t in_position) {
    out_data_[out_position_++] = values_data_[in_position];
  }

  void WriteValueSegment(int64_t in_start, int64_t length) {
    std::memcpy(out_data_ + out_position_, values_data_ + in_start, length * sizeof(T));
    out_position_ += length;
  }

  void WriteNull() {
    // Zero the memory
    out_data_[out_position_++] = T{};
  }

 private:
  const uint8_t* values_is_valid_;
  const T* values_data_;
  int64_t values_null_count_;
  int64_t values_offset_;
  int64_t values_length_;
  const uint8_t* filter_is_valid_;
  const uint8_t* filter_data_;
  int64_t filter_null_count_;
  int64_t filter_offset_;
  FilterOptions::NullSelectionBehavior null_selection_;
  uint8_t* out_is_valid_;
  T* out_data_;
  int64_t out_offset_;
  int64_t out_length_;
  int64_t out_position_;
};

template <>
inline void PrimitiveFilterImpl<BooleanType>::WriteValue(int64_t in_position) {
  bit_util::SetBitTo(out_data_, out_offset_ + out_position_++,
                     bit_util::GetBit(values_data_, values_offset_ + in_position));
}

template <>
inline void PrimitiveFilterImpl<BooleanType>::WriteValueSegment(int64_t in_start,
                                                                int64_t length) {
  CopyBitmap(values_data_, values_offset_ + in_start, length, out_data_,
             out_offset_ + out_position_);
  out_position_ += length;
}

template <>
inline void PrimitiveFilterImpl<BooleanType>::WriteNull() {
  // Zero the bit
  bit_util::ClearBit(out_data_, out_offset_ + out_position_++);
}

Status PrimitiveFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& values = batch[0].array;
  const ArraySpan& filter = batch[1].array;
  FilterOptions::NullSelectionBehavior null_selection =
      FilterState::Get(ctx).null_selection_behavior;

  int64_t output_length = GetFilterOutputSize(filter, null_selection);

  ArrayData* out_arr = out->array_data().get();

  // The output precomputed null count is unknown except in the narrow
  // condition that all the values are non-null and the filter will not cause
  // any new nulls to be created.
  if (values.null_count == 0 &&
      (null_selection == FilterOptions::DROP || filter.null_count == 0)) {
    out_arr->null_count = 0;
  } else {
    out_arr->null_count = kUnknownNullCount;
  }

  // When neither the values nor filter is known to have any nulls, we will
  // elect the optimized ExecNonNull path where there is no need to populate a
  // validity bitmap.
  bool allocate_validity = values.null_count != 0 || filter.null_count != 0;

  const int bit_width = values.type->bit_width();
  RETURN_NOT_OK(PreallocatePrimitiveArrayData(ctx, output_length, bit_width,
                                              allocate_validity, out_arr));

  switch (bit_width) {
    case 1:
      PrimitiveFilterImpl<BooleanType>(values, filter, null_selection, out_arr).Exec();
      break;
    case 8:
      PrimitiveFilterImpl<UInt8Type>(values, filter, null_selection, out_arr).Exec();
      break;
    case 16:
      PrimitiveFilterImpl<UInt16Type>(values, filter, null_selection, out_arr).Exec();
      break;
    case 32:
      PrimitiveFilterImpl<UInt32Type>(values, filter, null_selection, out_arr).Exec();
      break;
    case 64:
      PrimitiveFilterImpl<UInt64Type>(values, filter, null_selection, out_arr).Exec();
      break;
    default:
      DCHECK(false) << "Invalid values bit width";
      break;
  }
  return Status::OK();
}

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
template <typename Type>
Status BinaryFilterNonNullImpl(KernelContext* ctx, const ArraySpan& values,
                               const ArraySpan& filter, int64_t output_length,
                               FilterOptions::NullSelectionBehavior null_selection,
                               ArrayData* out) {
  using offset_type = typename Type::offset_type;
  const auto filter_data = filter.buffers[1].data;

  BINARY_FILTER_SETUP_COMMON();

  RETURN_NOT_OK(arrow::internal::VisitSetBitRuns(
      filter_data, filter.offset, filter.length, [&](int64_t position, int64_t length) {
        // Bulk-append raw data
        const offset_type run_data_bytes =
            (raw_offsets[position + length] - raw_offsets[position]);
        APPEND_RAW_DATA(raw_data + raw_offsets[position], run_data_bytes);
        // Append offsets
        offset_type cur_offset = raw_offsets[position];
        for (int64_t i = 0; i < length; ++i) {
          offset_builder.UnsafeAppend(offset);
          offset += raw_offsets[i + position + 1] - cur_offset;
          cur_offset = raw_offsets[i + position + 1];
        }
        return Status::OK();
      }));

  offset_builder.UnsafeAppend(offset);
  out->length = output_length;
  RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
  return data_builder.Finish(&out->buffers[2]);
}

template <typename Type>
Status BinaryFilterImpl(KernelContext* ctx, const ArraySpan& values,
                        const ArraySpan& filter, int64_t output_length,
                        FilterOptions::NullSelectionBehavior null_selection,
                        ArrayData* out) {
  using offset_type = typename Type::offset_type;

  const auto filter_data = filter.buffers[1].data;
  const uint8_t* filter_is_valid = filter.buffers[0].data;
  const int64_t filter_offset = filter.offset;

  const uint8_t* values_is_valid = values.buffers[0].data;
  const int64_t values_offset = values.offset;

  uint8_t* out_is_valid = out->buffers[0]->mutable_data();
  // Zero bits and then only have to set valid values to true
  bit_util::SetBitsTo(out_is_valid, 0, output_length, false);

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

  BINARY_FILTER_SETUP_COMMON();

  int64_t in_position = 0;
  int64_t out_position = 0;
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
          bit_util::SetBitsTo(out_is_valid, out_position, filter_block.length, true);

          // Bulk-append raw data
          offset_type block_data_bytes =
              (raw_offsets[in_position + filter_block.length] - raw_offsets[in_position]);
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
              bit_util::SetBit(out_is_valid, out_position);
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
              bit_util::SetBit(out_is_valid, out_position++);
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
                bit_util::SetBit(out_is_valid, out_position);
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
              bit_util::SetBit(out_is_valid, out_position++);
              APPEND_SINGLE_VALUE();
            }
          }
        } else {
          for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
            if (bit_util::GetBit(filter_is_valid, filter_offset + in_position) &&
                bit_util::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              if (bit_util::GetBit(values_is_valid, values_offset + in_position)) {
                bit_util::SetBit(out_is_valid, out_position);
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
              bit_util::SetBit(out_is_valid, out_position++);
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
                bit_util::SetBit(out_is_valid, out_position);
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
  int64_t output_length = GetFilterOutputSize(filter, null_selection);

  ArrayData* out_arr = out->array_data().get();

  // The output precomputed null count is unknown except in the narrow
  // condition that all the values are non-null and the filter will not cause
  // any new nulls to be created.
  if (values.null_count == 0 &&
      (null_selection == FilterOptions::DROP || filter.null_count == 0)) {
    out_arr->null_count = 0;
  } else {
    out_arr->null_count = kUnknownNullCount;
  }
  Type::type type_id = values.type->id();
  if (values.null_count == 0 && filter.null_count == 0) {
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

Status StructFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  // Transform filter to selection indices and then use Take.
  std::shared_ptr<ArrayData> indices;
  RETURN_NOT_OK(GetTakeIndices(batch[1].array,
                               FilterState::Get(ctx).null_selection_behavior,
                               ctx->memory_pool())
                    .Value(&indices));

  Datum result;
  RETURN_NOT_OK(Take(batch[0].array.ToArrayData(), Datum(indices),
                     TakeOptions::NoBoundsCheck(), ctx->exec_context())
                    .Value(&result));
  out->value = result.array();
  return Status::OK();
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

  // Convert filter to selection vector/indices and use Take
  const auto& filter_opts = *static_cast<const FilterOptions*>(options);
  ARROW_ASSIGN_OR_RAISE(
      std::shared_ptr<ArrayData> indices,
      GetTakeIndices(*filter.array(), filter_opts.null_selection_behavior,
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
      return Status::NotImplemented("Filter should be array-like");
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
    if (args[1].type()->id() != Type::BOOL) {
      return Status::NotImplemented("Filter argument must be boolean type");
    }

    if (args[0].kind() == Datum::RECORD_BATCH) {
      auto values_batch = args[0].record_batch();
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
  *out = {
      {InputType(match::Primitive()), PrimitiveFilterExec},
      {InputType(match::BinaryLike()), BinaryFilterExec},
      {InputType(match::LargeBinaryLike()), BinaryFilterExec},
      {InputType(Type::FIXED_SIZE_BINARY), FSBFilterExec},
      {InputType(null()), NullFilterExec},
      {InputType(Type::DECIMAL128), FSBFilterExec},
      {InputType(Type::DECIMAL256), FSBFilterExec},
      {InputType(Type::DICTIONARY), DictionaryFilterExec},
      {InputType(Type::EXTENSION), ExtensionFilterExec},
      {InputType(Type::LIST), ListFilterExec},
      {InputType(Type::LARGE_LIST), LargeListFilterExec},
      {InputType(Type::FIXED_SIZE_LIST), FSLFilterExec},
      {InputType(Type::DENSE_UNION), DenseUnionFilterExec},
      {InputType(Type::STRUCT), StructFilterExec},
      {InputType(Type::MAP), MapFilterExec},
  };
}

}  // namespace internal
}  // namespace compute

}  // namespace arrow
