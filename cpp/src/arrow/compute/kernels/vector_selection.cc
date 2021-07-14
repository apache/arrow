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

#include <algorithm>
#include <cstring>
#include <limits>

#include "arrow/array/array_base.h"
#include "arrow/array/array_binary.h"
#include "arrow/array/array_dict.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/extension_type.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/bitmap_ops.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CheckIndexBounds;
using internal::CopyBitmap;
using internal::CountSetBits;
using internal::GetArrayView;
using internal::GetByteWidth;
using internal::OptionalBitBlockCounter;
using internal::OptionalBitIndexer;

namespace compute {
namespace internal {

int64_t GetFilterOutputSize(const ArrayData& filter,
                            FilterOptions::NullSelectionBehavior null_selection) {
  int64_t output_size = 0;

  if (filter.MayHaveNulls()) {
    const uint8_t* filter_is_valid = filter.buffers[0]->data();
    BinaryBitBlockCounter bit_counter(filter.buffers[1]->data(), filter.offset,
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
    output_size = CountSetBits(filter.buffers[1]->data(), filter.offset, filter.length);
  }
  return output_size;
}

namespace {

template <typename IndexType>
Result<std::shared_ptr<ArrayData>> GetTakeIndicesImpl(
    const ArrayData& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  using T = typename IndexType::c_type;

  const uint8_t* filter_data = filter.buffers[1]->data();
  const bool have_filter_nulls = filter.MayHaveNulls();
  const uint8_t* filter_is_valid =
      have_filter_nulls ? filter.buffers[0]->data() : nullptr;

  if (have_filter_nulls && null_selection == FilterOptions::EMIT_NULL) {
    // Most complex case: the filter may have nulls and we don't drop them.
    // The logic is ternary:
    // - filter is null: emit null
    // - filter is valid and true: emit index
    // - filter is valid and false: don't emit anything

    typename TypeTraits<IndexType>::BuilderType builder(memory_pool);

    // The position relative to the start of the filter
    T position = 0;
    // The current position taking the filter offset into account
    int64_t position_with_offset = filter.offset;

    // To count blocks where filter_data[i] || !filter_is_valid[i]
    BinaryBitBlockCounter filter_counter(filter_data, filter.offset, filter_is_valid,
                                         filter.offset, filter.length);
    BitBlockCounter is_valid_counter(filter_is_valid, filter.offset, filter.length);
    while (position < filter.length) {
      // true OR NOT valid
      BitBlockCount selected_or_null_block = filter_counter.NextOrNotWord();
      if (selected_or_null_block.NoneSet()) {
        position += selected_or_null_block.length;
        position_with_offset += selected_or_null_block.length;
        continue;
      }
      RETURN_NOT_OK(builder.Reserve(selected_or_null_block.popcount));

      // If the values are all valid and the selected_or_null_block is full,
      // then we can infer that all the values are true and skip the bit checking
      BitBlockCount is_valid_block = is_valid_counter.NextWord();

      if (selected_or_null_block.AllSet() && is_valid_block.AllSet()) {
        // All the values are selected and non-null
        for (int64_t i = 0; i < selected_or_null_block.length; ++i) {
          builder.UnsafeAppend(position++);
        }
        position_with_offset += selected_or_null_block.length;
      } else {
        // Some of the values are false or null
        for (int64_t i = 0; i < selected_or_null_block.length; ++i) {
          if (BitUtil::GetBit(filter_is_valid, position_with_offset)) {
            if (BitUtil::GetBit(filter_data, position_with_offset)) {
              builder.UnsafeAppend(position);
            }
          } else {
            // Null slot, so append a null
            builder.UnsafeAppendNull();
          }
          ++position;
          ++position_with_offset;
        }
      }
    }
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(builder.FinishInternal(&result));
    return result;
  }

  // Other cases don't emit nulls and are therefore simpler.
  TypedBufferBuilder<T> builder(memory_pool);

  if (have_filter_nulls) {
    // The filter may have nulls, so we scan the validity bitmap and the filter
    // data bitmap together.
    DCHECK_EQ(null_selection, FilterOptions::DROP);

    // The position relative to the start of the filter
    T position = 0;
    // The current position taking the filter offset into account
    int64_t position_with_offset = filter.offset;

    BinaryBitBlockCounter filter_counter(filter_data, filter.offset, filter_is_valid,
                                         filter.offset, filter.length);
    while (position < filter.length) {
      BitBlockCount and_block = filter_counter.NextAndWord();
      RETURN_NOT_OK(builder.Reserve(and_block.popcount));
      if (and_block.AllSet()) {
        // All the values are selected and non-null
        for (int64_t i = 0; i < and_block.length; ++i) {
          builder.UnsafeAppend(position++);
        }
        position_with_offset += and_block.length;
      } else if (!and_block.NoneSet()) {
        // Some of the values are false or null
        for (int64_t i = 0; i < and_block.length; ++i) {
          if (BitUtil::GetBit(filter_is_valid, position_with_offset) &&
              BitUtil::GetBit(filter_data, position_with_offset)) {
            builder.UnsafeAppend(position);
          }
          ++position;
          ++position_with_offset;
        }
      } else {
        position += and_block.length;
        position_with_offset += and_block.length;
      }
    }
  } else {
    // The filter has no nulls, so we need only look for true values
    RETURN_NOT_OK(::arrow::internal::VisitSetBitRuns(
        filter_data, filter.offset, filter.length, [&](int64_t offset, int64_t length) {
          // Append the consecutive run of indices
          RETURN_NOT_OK(builder.Reserve(length));
          for (int64_t i = 0; i < length; ++i) {
            builder.UnsafeAppend(static_cast<T>(offset + i));
          }
          return Status::OK();
        }));
  }

  const int64_t length = builder.length();
  std::shared_ptr<Buffer> out_buffer;
  RETURN_NOT_OK(builder.Finish(&out_buffer));
  return std::make_shared<ArrayData>(TypeTraits<IndexType>::type_singleton(), length,
                                     BufferVector{nullptr, out_buffer}, /*null_count=*/0);
}

}  // namespace

Result<std::shared_ptr<ArrayData>> GetTakeIndices(
    const ArrayData& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  DCHECK_EQ(filter.type->id(), Type::BOOL);
  if (filter.length <= std::numeric_limits<uint16_t>::max()) {
    return GetTakeIndicesImpl<UInt16Type>(filter, null_selection, memory_pool);
  } else if (filter.length <= std::numeric_limits<uint32_t>::max()) {
    return GetTakeIndicesImpl<UInt32Type>(filter, null_selection, memory_pool);
  } else {
    // Arrays over 4 billion elements, not especially likely.
    return Status::NotImplemented(
        "Filter length exceeds UINT32_MAX, "
        "consider a different strategy for selecting elements");
  }
}

namespace {

using FilterState = OptionsWrapper<FilterOptions>;
using TakeState = OptionsWrapper<TakeOptions>;

Status PreallocateData(KernelContext* ctx, int64_t length, int bit_width,
                       bool allocate_validity, ArrayData* out) {
  // Preallocate memory
  out->length = length;
  out->buffers.resize(2);

  if (allocate_validity) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[0], ctx->AllocateBitmap(length));
  }
  if (bit_width == 1) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->AllocateBitmap(length));
  } else {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->Allocate(length * bit_width / 8));
  }
  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement optimized take for primitive types from boolean to 1/2/4/8-byte
// C-type based types. Use common implementation for every byte width and only
// generate code for unsigned integer indices, since after boundschecking to
// check for negative numbers in the indices we can safely reinterpret_cast
// signed integers as unsigned.

/// \brief The Take implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather the physical C type. This way we
/// only generate one take function for each byte width.
///
/// This function assumes that the indices have been boundschecked.
template <typename IndexCType, typename ValueCType>
struct PrimitiveTakeImpl {
  static void Exec(const PrimitiveArg& values, const PrimitiveArg& indices,
                   ArrayData* out_arr) {
    auto values_data = reinterpret_cast<const ValueCType*>(values.data);
    auto values_is_valid = values.is_valid;
    auto values_offset = values.offset;

    auto indices_data = reinterpret_cast<const IndexCType*>(indices.data);
    auto indices_is_valid = indices.is_valid;
    auto indices_offset = indices.offset;

    auto out = out_arr->GetMutableValues<ValueCType>(1);
    auto out_is_valid = out_arr->buffers[0]->mutable_data();
    auto out_offset = out_arr->offset;

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (values.null_count != 0 || indices.null_count != 0) {
      BitUtil::SetBitsTo(out_is_valid, out_offset, indices.length, false);
    }

    OptionalBitBlockCounter indices_bit_counter(indices_is_valid, indices_offset,
                                                indices.length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < indices.length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (values.null_count == 0) {
        // Values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither values nor index nulls
          BitUtil::SetBitsTo(out_is_valid, out_offset + position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            out[position] = values_data[indices_data[position]];
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              BitUtil::SetBit(out_is_valid, out_offset + position);
              out[position] = values_data[indices_data[position]];
            } else {
              out[position] = ValueCType{};
            }
            ++position;
          }
        } else {
          memset(out + position, 0, sizeof(ValueCType) * block.length);
          position += block.length;
        }
      } else {
        // Values have nulls, so we must do random access into the values bitmap
        if (block.popcount == block.length) {
          // Faster path: indices are not null but values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(values_is_valid,
                                values_offset + indices_data[position])) {
              // value is not null
              out[position] = values_data[indices_data[position]];
              BitUtil::SetBit(out_is_valid, out_offset + position);
              ++valid_count;
            } else {
              out[position] = ValueCType{};
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_is_valid, indices_offset + position) &&
                BitUtil::GetBit(values_is_valid,
                                values_offset + indices_data[position])) {
              // index is not null && value is not null
              out[position] = values_data[indices_data[position]];
              BitUtil::SetBit(out_is_valid, out_offset + position);
              ++valid_count;
            } else {
              out[position] = ValueCType{};
            }
            ++position;
          }
        } else {
          memset(out + position, 0, sizeof(ValueCType) * block.length);
          position += block.length;
        }
      }
    }
    out_arr->null_count = out_arr->length - valid_count;
  }
};

template <typename IndexCType>
struct BooleanTakeImpl {
  static void Exec(const PrimitiveArg& values, const PrimitiveArg& indices,
                   ArrayData* out_arr) {
    const uint8_t* values_data = values.data;
    auto values_is_valid = values.is_valid;
    auto values_offset = values.offset;

    auto indices_data = reinterpret_cast<const IndexCType*>(indices.data);
    auto indices_is_valid = indices.is_valid;
    auto indices_offset = indices.offset;

    auto out = out_arr->buffers[1]->mutable_data();
    auto out_is_valid = out_arr->buffers[0]->mutable_data();
    auto out_offset = out_arr->offset;

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (values.null_count != 0 || indices.null_count != 0) {
      BitUtil::SetBitsTo(out_is_valid, out_offset, indices.length, false);
    }
    // Avoid uninitialized data in values array
    BitUtil::SetBitsTo(out, out_offset, indices.length, false);

    auto PlaceDataBit = [&](int64_t loc, IndexCType index) {
      BitUtil::SetBitTo(out, out_offset + loc,
                        BitUtil::GetBit(values_data, values_offset + index));
    };

    OptionalBitBlockCounter indices_bit_counter(indices_is_valid, indices_offset,
                                                indices.length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < indices.length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (values.null_count == 0) {
        // Values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither values nor index nulls
          BitUtil::SetBitsTo(out_is_valid, out_offset + position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            PlaceDataBit(position, indices_data[position]);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              BitUtil::SetBit(out_is_valid, out_offset + position);
              PlaceDataBit(position, indices_data[position]);
            }
            ++position;
          }
        } else {
          position += block.length;
        }
      } else {
        // Values have nulls, so we must do random access into the values bitmap
        if (block.popcount == block.length) {
          // Faster path: indices are not null but values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(values_is_valid,
                                values_offset + indices_data[position])) {
              // value is not null
              BitUtil::SetBit(out_is_valid, out_offset + position);
              PlaceDataBit(position, indices_data[position]);
              ++valid_count;
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              if (BitUtil::GetBit(values_is_valid,
                                  values_offset + indices_data[position])) {
                // value is not null
                PlaceDataBit(position, indices_data[position]);
                BitUtil::SetBit(out_is_valid, out_offset + position);
                ++valid_count;
              }
            }
            ++position;
          }
        } else {
          position += block.length;
        }
      }
    }
    out_arr->null_count = out_arr->length - valid_count;
  }
};

template <template <typename...> class TakeImpl, typename... Args>
void TakeIndexDispatch(const PrimitiveArg& values, const PrimitiveArg& indices,
                       ArrayData* out) {
  // With the simplifying assumption that boundschecking has taken place
  // already at a higher level, we can now assume that the index values are all
  // non-negative. Thus, we can interpret signed integers as unsigned and avoid
  // having to generate double the amount of binary code to handle each integer
  // width.
  switch (indices.bit_width) {
    case 8:
      return TakeImpl<uint8_t, Args...>::Exec(values, indices, out);
    case 16:
      return TakeImpl<uint16_t, Args...>::Exec(values, indices, out);
    case 32:
      return TakeImpl<uint32_t, Args...>::Exec(values, indices, out);
    case 64:
      return TakeImpl<uint64_t, Args...>::Exec(values, indices, out);
    default:
      DCHECK(false) << "Invalid indices byte width";
      break;
  }
}

Status PrimitiveTake(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(*batch[1].array(), batch[0].length()));
  }

  PrimitiveArg values = GetPrimitiveArg(*batch[0].array());
  PrimitiveArg indices = GetPrimitiveArg(*batch[1].array());

  ArrayData* out_arr = out->mutable_array();

  // TODO: When neither values nor indices contain nulls, we can skip
  // allocating the validity bitmap altogether and save time and space. A
  // streamlined PrimitiveTakeImpl would need to be written that skips all
  // interactions with the output validity bitmap, though.
  RETURN_NOT_OK(PreallocateData(ctx, indices.length, values.bit_width,
                                /*allocate_validity=*/true, out_arr));
  switch (values.bit_width) {
    case 1:
      TakeIndexDispatch<BooleanTakeImpl>(values, indices, out_arr);
      break;
    case 8:
      TakeIndexDispatch<PrimitiveTakeImpl, int8_t>(values, indices, out_arr);
      break;
    case 16:
      TakeIndexDispatch<PrimitiveTakeImpl, int16_t>(values, indices, out_arr);
      break;
    case 32:
      TakeIndexDispatch<PrimitiveTakeImpl, int32_t>(values, indices, out_arr);
      break;
    case 64:
      TakeIndexDispatch<PrimitiveTakeImpl, int64_t>(values, indices, out_arr);
      break;
    default:
      DCHECK(false) << "Invalid values byte width";
      break;
  }
  return Status::OK();
}

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

  PrimitiveFilterImpl(const PrimitiveArg& values, const PrimitiveArg& filter,
                      FilterOptions::NullSelectionBehavior null_selection,
                      ArrayData* out_arr)
      : values_is_valid_(values.is_valid),
        values_data_(reinterpret_cast<const T*>(values.data)),
        values_null_count_(values.null_count),
        values_offset_(values.offset),
        values_length_(values.length),
        filter_is_valid_(filter.is_valid),
        filter_data_(filter.data),
        filter_null_count_(filter.null_count),
        filter_offset_(filter.offset),
        null_selection_(null_selection) {
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
      BitUtil::SetBit(out_is_valid_, out_offset_ + out_position_);
      // Increments out_position_
      WriteValue(index);
    };

    auto WriteMaybeNull = [&](int64_t index) {
      BitUtil::SetBitTo(out_is_valid_, out_offset_ + out_position_,
                        BitUtil::GetBit(values_is_valid_, values_offset_ + index));
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
        BitUtil::SetBitsTo(out_is_valid_, out_offset_ + out_position_,
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
              if (BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteNotNull(in_position);
              }
              ++in_position;
            }
          } else if (null_selection_ == FilterOptions::DROP) {
            // If any values are selected, they ARE NOT null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (BitUtil::GetBit(filter_is_valid_, filter_offset_ + in_position) &&
                  BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteNotNull(in_position);
              }
              ++in_position;
            }
          } else {  // null_selection == FilterOptions::EMIT_NULL
            // Data values in this block are not null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              const bool is_valid =
                  BitUtil::GetBit(filter_is_valid_, filter_offset_ + in_position);
              if (is_valid &&
                  BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                // Filter slot is non-null and set
                WriteNotNull(in_position);
              } else if (!is_valid) {
                // Filter slot is null, so we have a null in the output
                BitUtil::ClearBit(out_is_valid_, out_offset_ + out_position_);
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
              if (BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteMaybeNull(in_position);
              }
              ++in_position;
            }
          } else if (null_selection_ == FilterOptions::DROP) {
            // If any values are selected, they ARE NOT null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (BitUtil::GetBit(filter_is_valid_, filter_offset_ + in_position) &&
                  BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                WriteMaybeNull(in_position);
              }
              ++in_position;
            }
          } else {  // null_selection == FilterOptions::EMIT_NULL
            // Data values in this block are not null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              const bool is_valid =
                  BitUtil::GetBit(filter_is_valid_, filter_offset_ + in_position);
              if (is_valid &&
                  BitUtil::GetBit(filter_data_, filter_offset_ + in_position)) {
                // Filter slot is non-null and set
                WriteMaybeNull(in_position);
              } else if (!is_valid) {
                // Filter slot is null, so we have a null in the output
                BitUtil::ClearBit(out_is_valid_, out_offset_ + out_position_);
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
  BitUtil::SetBitTo(out_data_, out_offset_ + out_position_++,
                    BitUtil::GetBit(values_data_, values_offset_ + in_position));
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
  BitUtil::ClearBit(out_data_, out_offset_ + out_position_++);
}

Status PrimitiveFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  PrimitiveArg values = GetPrimitiveArg(*batch[0].array());
  PrimitiveArg filter = GetPrimitiveArg(*batch[1].array());
  FilterOptions::NullSelectionBehavior null_selection =
      FilterState::Get(ctx).null_selection_behavior;

  int64_t output_length = GetFilterOutputSize(*batch[1].array(), null_selection);

  ArrayData* out_arr = out->mutable_array();

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

  RETURN_NOT_OK(
      PreallocateData(ctx, output_length, values.bit_width, allocate_validity, out_arr));

  switch (values.bit_width) {
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
  auto raw_offsets =                                                                    \
      reinterpret_cast<const offset_type*>(values.buffers[1]->data()) + values.offset;  \
  const uint8_t* raw_data = values.buffers[2]->data();                                  \
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
Status BinaryFilterNonNullImpl(KernelContext* ctx, const ArrayData& values,
                               const ArrayData& filter, int64_t output_length,
                               FilterOptions::NullSelectionBehavior null_selection,
                               ArrayData* out) {
  using offset_type = typename Type::offset_type;
  const auto filter_data = filter.buffers[1]->data();

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
Status BinaryFilterImpl(KernelContext* ctx, const ArrayData& values,
                        const ArrayData& filter, int64_t output_length,
                        FilterOptions::NullSelectionBehavior null_selection,
                        ArrayData* out) {
  using offset_type = typename Type::offset_type;

  const auto filter_data = filter.buffers[1]->data();
  const uint8_t* filter_is_valid = GetValidityBitmap(filter);
  const int64_t filter_offset = filter.offset;

  const uint8_t* values_is_valid = GetValidityBitmap(values);
  const int64_t values_offset = values.offset;

  uint8_t* out_is_valid = out->buffers[0]->mutable_data();
  // Zero bits and then only have to set valid values to true
  BitUtil::SetBitsTo(out_is_valid, 0, output_length, false);

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
          BitUtil::SetBitsTo(out_is_valid, out_position, filter_block.length, true);

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
            if (BitUtil::GetBit(values_is_valid, values_offset + in_position)) {
              BitUtil::SetBit(out_is_valid, out_position);
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
            if (BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              BitUtil::SetBit(out_is_valid, out_position++);
              APPEND_SINGLE_VALUE();
            }
          }
        } else {
          // Some of the values in the block are null, so we have to check
          // each one
          for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
            if (BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              if (BitUtil::GetBit(values_is_valid, values_offset + in_position)) {
                BitUtil::SetBit(out_is_valid, out_position);
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
            if (BitUtil::GetBit(filter_is_valid, filter_offset + in_position) &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              BitUtil::SetBit(out_is_valid, out_position++);
              APPEND_SINGLE_VALUE();
            }
          }
        } else {
          for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
            if (BitUtil::GetBit(filter_is_valid, filter_offset + in_position) &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              if (BitUtil::GetBit(values_is_valid, values_offset + in_position)) {
                BitUtil::SetBit(out_is_valid, out_position);
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
                BitUtil::GetBit(filter_is_valid, filter_offset + in_position);
            if (filter_not_null &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              BitUtil::SetBit(out_is_valid, out_position++);
              APPEND_SINGLE_VALUE();
            } else if (!filter_not_null) {
              offset_builder.UnsafeAppend(offset);
              ++out_position;
            }
          }
        } else {
          for (int64_t i = 0; i < filter_block.length; ++i, ++in_position) {
            const bool filter_not_null =
                BitUtil::GetBit(filter_is_valid, filter_offset + in_position);
            if (filter_not_null &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              offset_builder.UnsafeAppend(offset);
              if (BitUtil::GetBit(values_is_valid, values_offset + in_position)) {
                BitUtil::SetBit(out_is_valid, out_position);
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

Status BinaryFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  FilterOptions::NullSelectionBehavior null_selection =
      FilterState::Get(ctx).null_selection_behavior;

  const ArrayData& values = *batch[0].array();
  const ArrayData& filter = *batch[1].array();
  int64_t output_length = GetFilterOutputSize(filter, null_selection);
  ArrayData* out_arr = out->mutable_array();

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
// Null take and filter

Status NullTake(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(*batch[1].array(), batch[0].length()));
  }
  // batch.length doesn't take into account the take indices
  auto new_length = batch[1].array()->length;
  out->value = std::make_shared<NullArray>(new_length)->data();
  return Status::OK();
}

Status NullFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  int64_t output_length = GetFilterOutputSize(
      *batch[1].array(), FilterState::Get(ctx).null_selection_behavior);
  out->value = std::make_shared<NullArray>(output_length)->data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Dictionary take and filter

Status DictionaryTake(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  DictionaryArray values(batch[0].array());
  Datum result;
  RETURN_NOT_OK(
      Take(Datum(values.indices()), batch[1], TakeState::Get(ctx), ctx->exec_context())
          .Value(&result));
  DictionaryArray taken_values(values.type(), result.make_array(), values.dictionary());
  out->value = taken_values.data();
  return Status::OK();
}

Status DictionaryFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  DictionaryArray dict_values(batch[0].array());
  Datum result;
  RETURN_NOT_OK(Filter(Datum(dict_values.indices()), batch[1].array(),
                       FilterState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  DictionaryArray filtered_values(dict_values.type(), result.make_array(),
                                  dict_values.dictionary());
  out->value = filtered_values.data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Extension take and filter

Status ExtensionTake(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ExtensionArray values(batch[0].array());
  Datum result;
  RETURN_NOT_OK(
      Take(Datum(values.storage()), batch[1], TakeState::Get(ctx), ctx->exec_context())
          .Value(&result));
  ExtensionArray taken_values(values.type(), result.make_array());
  out->value = taken_values.data();
  return Status::OK();
}

Status ExtensionFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ExtensionArray ext_values(batch[0].array());
  Datum result;
  RETURN_NOT_OK(Filter(Datum(ext_values.storage()), batch[1].array(),
                       FilterState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  ExtensionArray filtered_values(ext_values.type(), result.make_array());
  out->value = filtered_values.data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Implement take for other data types where there is less performance
// sensitivity by visiting the selected indices.

// Use CRTP to dispatch to type-specific processing of take indices for each
// unsigned integer type.
template <typename Impl, typename Type>
struct Selection {
  using ValuesArrayType = typename TypeTraits<Type>::ArrayType;

  // Forwards the generic value visitors to the take index visitor template
  template <typename IndexCType>
  struct TakeAdapter {
    static constexpr bool is_take = true;

    Impl* impl;
    explicit TakeAdapter(Impl* impl) : impl(impl) {}
    template <typename ValidVisitor, typename NullVisitor>
    Status Generate(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
      return impl->template VisitTake<IndexCType>(std::forward<ValidVisitor>(visit_valid),
                                                  std::forward<NullVisitor>(visit_null));
    }
  };

  // Forwards the generic value visitors to the VisitFilter template
  struct FilterAdapter {
    static constexpr bool is_take = false;

    Impl* impl;
    explicit FilterAdapter(Impl* impl) : impl(impl) {}
    template <typename ValidVisitor, typename NullVisitor>
    Status Generate(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
      return impl->VisitFilter(std::forward<ValidVisitor>(visit_valid),
                               std::forward<NullVisitor>(visit_null));
    }
  };

  KernelContext* ctx;
  std::shared_ptr<ArrayData> values;
  std::shared_ptr<ArrayData> selection;
  int64_t output_length;
  ArrayData* out;
  TypedBufferBuilder<bool> validity_builder;

  Selection(KernelContext* ctx, const ExecBatch& batch, int64_t output_length, Datum* out)
      : ctx(ctx),
        values(batch[0].array()),
        selection(batch[1].array()),
        output_length(output_length),
        out(out->mutable_array()),
        validity_builder(ctx->memory_pool()) {}

  virtual ~Selection() = default;

  Status FinishCommon() {
    out->buffers.resize(values->buffers.size());
    out->length = validity_builder.length();
    out->null_count = validity_builder.false_count();
    return validity_builder.Finish(&out->buffers[0]);
  }

  template <typename IndexCType, typename ValidVisitor, typename NullVisitor>
  Status VisitTake(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
    const auto indices_values = selection->GetValues<IndexCType>(1);
    const uint8_t* is_valid = GetValidityBitmap(*selection);
    OptionalBitIndexer indices_is_valid(selection->buffers[0], selection->offset);
    OptionalBitIndexer values_is_valid(values->buffers[0], values->offset);

    const bool values_have_nulls = values->MayHaveNulls();
    OptionalBitBlockCounter bit_counter(is_valid, selection->offset, selection->length);
    int64_t position = 0;
    while (position < selection->length) {
      BitBlockCount block = bit_counter.NextBlock();
      const bool indices_have_nulls = block.popcount < block.length;
      if (!indices_have_nulls && !values_have_nulls) {
        // Fastest path, neither indices nor values have nulls
        validity_builder.UnsafeAppend(block.length, true);
        for (int64_t i = 0; i < block.length; ++i) {
          RETURN_NOT_OK(visit_valid(indices_values[position++]));
        }
      } else if (block.popcount > 0) {
        // Since we have to branch on whether the indices are null or not, we
        // combine the "non-null indices block but some values null" and
        // "some-null indices block but values non-null" into a single loop.
        for (int64_t i = 0; i < block.length; ++i) {
          if ((!indices_have_nulls || indices_is_valid[position]) &&
              values_is_valid[indices_values[position]]) {
            validity_builder.UnsafeAppend(true);
            RETURN_NOT_OK(visit_valid(indices_values[position]));
          } else {
            validity_builder.UnsafeAppend(false);
            RETURN_NOT_OK(visit_null());
          }
          ++position;
        }
      } else {
        // The whole block is null
        validity_builder.UnsafeAppend(block.length, false);
        for (int64_t i = 0; i < block.length; ++i) {
          RETURN_NOT_OK(visit_null());
        }
        position += block.length;
      }
    }
    return Status::OK();
  }

  // We use the NullVisitor both for "selected" nulls as well as "emitted"
  // nulls coming from the filter when using FilterOptions::EMIT_NULL
  template <typename ValidVisitor, typename NullVisitor>
  Status VisitFilter(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
    auto null_selection = FilterState::Get(ctx).null_selection_behavior;

    const auto filter_data = selection->buffers[1]->data();

    const uint8_t* filter_is_valid = GetValidityBitmap(*selection);
    const int64_t filter_offset = selection->offset;
    OptionalBitIndexer values_is_valid(values->buffers[0], values->offset);

    // We use 3 block counters for fast scanning of the filter
    //
    // * values_valid_counter: for values null/not-null
    // * filter_valid_counter: for filter null/not-null
    // * filter_counter: for filter true/false
    OptionalBitBlockCounter values_valid_counter(GetValidityBitmap(*values),
                                                 values->offset, values->length);
    OptionalBitBlockCounter filter_valid_counter(filter_is_valid, filter_offset,
                                                 selection->length);
    BitBlockCounter filter_counter(filter_data, filter_offset, selection->length);
    int64_t in_position = 0;

    auto AppendNotNull = [&](int64_t index) -> Status {
      validity_builder.UnsafeAppend(true);
      return visit_valid(index);
    };

    auto AppendNull = [&]() -> Status {
      validity_builder.UnsafeAppend(false);
      return visit_null();
    };

    auto AppendMaybeNull = [&](int64_t index) -> Status {
      if (values_is_valid[index]) {
        return AppendNotNull(index);
      } else {
        return AppendNull();
      }
    };

    while (in_position < selection->length) {
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
            validity_builder.UnsafeAppend(filter_block.length, true);
            for (int64_t i = 0; i < filter_block.length; ++i) {
              RETURN_NOT_OK(visit_valid(in_position++));
            }
          } else {
            // Some of the values in this block are null
            for (int64_t i = 0; i < filter_block.length; ++i) {
              RETURN_NOT_OK(AppendMaybeNull(in_position++));
            }
          }
        } else {  // !filter_block.AllSet()
          // Some of the filter values are false, but all not null
          if (values_valid_block.AllSet()) {
            // All the values are not-null, so we can skip null checking for
            // them
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (BitUtil::GetBit(filter_data, filter_offset + in_position)) {
                RETURN_NOT_OK(AppendNotNull(in_position));
              }
              ++in_position;
            }
          } else {
            // Some of the values in the block are null, so we have to check
            // each one
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (BitUtil::GetBit(filter_data, filter_offset + in_position)) {
                RETURN_NOT_OK(AppendMaybeNull(in_position));
              }
              ++in_position;
            }
          }
        }
      } else {  // !filter_valid_block.AllSet()
        // Some of the filter values are null, so we have to handle the DROP
        // versus EMIT_NULL null selection behavior.
        if (null_selection == FilterOptions::DROP) {
          // Filter null values are treated as false.
          for (int64_t i = 0; i < filter_block.length; ++i) {
            if (BitUtil::GetBit(filter_is_valid, filter_offset + in_position) &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              RETURN_NOT_OK(AppendMaybeNull(in_position));
            }
            ++in_position;
          }
        } else {
          // Filter null values are appended to output as null whether the
          // value in the corresponding slot is valid or not
          for (int64_t i = 0; i < filter_block.length; ++i) {
            const bool filter_not_null =
                BitUtil::GetBit(filter_is_valid, filter_offset + in_position);
            if (filter_not_null &&
                BitUtil::GetBit(filter_data, filter_offset + in_position)) {
              RETURN_NOT_OK(AppendMaybeNull(in_position));
            } else if (!filter_not_null) {
              // EMIT_NULL case
              RETURN_NOT_OK(AppendNull());
            }
            ++in_position;
          }
        }
      }
    }
    return Status::OK();
  }

  virtual Status Init() { return Status::OK(); }

  // Implementation specific finish logic
  virtual Status Finish() = 0;

  Status ExecTake() {
    RETURN_NOT_OK(this->validity_builder.Reserve(output_length));
    RETURN_NOT_OK(Init());
    int index_width = GetByteWidth(*this->selection->type);

    // CTRP dispatch here
    switch (index_width) {
      case 1: {
        Status s =
            static_cast<Impl*>(this)->template GenerateOutput<TakeAdapter<uint8_t>>();
        RETURN_NOT_OK(s);
      } break;
      case 2: {
        Status s =
            static_cast<Impl*>(this)->template GenerateOutput<TakeAdapter<uint16_t>>();
        RETURN_NOT_OK(s);
      } break;
      case 4: {
        Status s =
            static_cast<Impl*>(this)->template GenerateOutput<TakeAdapter<uint32_t>>();
        RETURN_NOT_OK(s);
      } break;
      case 8: {
        Status s =
            static_cast<Impl*>(this)->template GenerateOutput<TakeAdapter<uint64_t>>();
        RETURN_NOT_OK(s);
      } break;
      default:
        DCHECK(false) << "Invalid index width";
        break;
    }
    RETURN_NOT_OK(this->FinishCommon());
    return Finish();
  }

  Status ExecFilter() {
    RETURN_NOT_OK(this->validity_builder.Reserve(output_length));
    RETURN_NOT_OK(Init());
    // CRTP dispatch
    Status s = static_cast<Impl*>(this)->template GenerateOutput<FilterAdapter>();
    RETURN_NOT_OK(s);
    RETURN_NOT_OK(this->FinishCommon());
    return Finish();
  }
};

#define LIFT_BASE_MEMBERS()                               \
  using ValuesArrayType = typename Base::ValuesArrayType; \
  using Base::ctx;                                        \
  using Base::values;                                     \
  using Base::selection;                                  \
  using Base::output_length;                              \
  using Base::out;                                        \
  using Base::validity_builder

static inline Status VisitNoop() { return Status::OK(); }

// A selection implementation for 32-bit and 64-bit variable binary
// types. Common generated kernels are shared between Binary/String and
// LargeBinary/LargeString
template <typename Type>
struct VarBinaryImpl : public Selection<VarBinaryImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = Selection<VarBinaryImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  std::shared_ptr<ArrayData> values_as_binary;
  TypedBufferBuilder<offset_type> offset_builder;
  TypedBufferBuilder<uint8_t> data_builder;

  static constexpr int64_t kOffsetLimit = std::numeric_limits<offset_type>::max() - 1;

  VarBinaryImpl(KernelContext* ctx, const ExecBatch& batch, int64_t output_length,
                Datum* out)
      : Base(ctx, batch, output_length, out),
        offset_builder(ctx->memory_pool()),
        data_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    ValuesArrayType typed_values(this->values_as_binary);

    // Presize the data builder with a rough estimate of the required data size
    if (values->length > 0) {
      const double mean_value_length =
          (typed_values.total_values_length() / static_cast<double>(values->length));

      // TODO: See if possible to reduce output_length for take/filter cases
      // where there are nulls in the selection array
      RETURN_NOT_OK(
          data_builder.Reserve(static_cast<int64_t>(mean_value_length * output_length)));
    }
    int64_t space_available = data_builder.capacity();

    const offset_type* raw_offsets = typed_values.raw_value_offsets();
    const uint8_t* raw_data = typed_values.raw_data();

    offset_type offset = 0;
    Adapter adapter(this);
    RETURN_NOT_OK(adapter.Generate(
        [&](int64_t index) {
          offset_builder.UnsafeAppend(offset);
          offset_type val_offset = raw_offsets[index];
          offset_type val_size = raw_offsets[index + 1] - val_offset;

          // Use static property to prune this code from the filter path in
          // optimized builds
          if (Adapter::is_take &&
              ARROW_PREDICT_FALSE(static_cast<int64_t>(offset) +
                                  static_cast<int64_t>(val_size)) > kOffsetLimit) {
            return Status::Invalid("Take operation overflowed binary array capacity");
          }
          offset += val_size;
          if (ARROW_PREDICT_FALSE(val_size > space_available)) {
            RETURN_NOT_OK(data_builder.Reserve(val_size));
            space_available = data_builder.capacity() - data_builder.length();
          }
          data_builder.UnsafeAppend(raw_data + val_offset, val_size);
          space_available -= val_size;
          return Status::OK();
        },
        [&]() {
          offset_builder.UnsafeAppend(offset);
          return Status::OK();
        }));
    offset_builder.UnsafeAppend(offset);
    return Status::OK();
  }

  Status Init() override {
    ARROW_ASSIGN_OR_RAISE(this->values_as_binary,
                          GetArrayView(this->values, TypeTraits<Type>::type_singleton()));
    return offset_builder.Reserve(output_length + 1);
  }

  Status Finish() override {
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    return data_builder.Finish(&out->buffers[2]);
  }
};

struct FSBImpl : public Selection<FSBImpl, FixedSizeBinaryType> {
  using Base = Selection<FSBImpl, FixedSizeBinaryType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<uint8_t> data_builder;

  FSBImpl(KernelContext* ctx, const ExecBatch& batch, int64_t output_length, Datum* out)
      : Base(ctx, batch, output_length, out), data_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    FixedSizeBinaryArray typed_values(this->values);
    int32_t value_size = typed_values.byte_width();

    RETURN_NOT_OK(data_builder.Reserve(value_size * output_length));
    Adapter adapter(this);
    return adapter.Generate(
        [&](int64_t index) {
          auto val = typed_values.GetView(index);
          data_builder.UnsafeAppend(reinterpret_cast<const uint8_t*>(val.data()),
                                    value_size);
          return Status::OK();
        },
        [&]() {
          data_builder.UnsafeAppend(value_size, static_cast<uint8_t>(0x00));
          return Status::OK();
        });
  }

  Status Finish() override { return data_builder.Finish(&out->buffers[1]); }
};

template <typename Type>
struct ListImpl : public Selection<ListImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = Selection<ListImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<offset_type> offset_builder;
  typename TypeTraits<Type>::OffsetBuilderType child_index_builder;

  ListImpl(KernelContext* ctx, const ExecBatch& batch, int64_t output_length, Datum* out)
      : Base(ctx, batch, output_length, out),
        offset_builder(ctx->memory_pool()),
        child_index_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    ValuesArrayType typed_values(this->values);

    // TODO presize child_index_builder with a similar heuristic as VarBinaryImpl

    offset_type offset = 0;
    Adapter adapter(this);
    RETURN_NOT_OK(adapter.Generate(
        [&](int64_t index) {
          offset_builder.UnsafeAppend(offset);
          offset_type value_offset = typed_values.value_offset(index);
          offset_type value_length = typed_values.value_length(index);
          offset += value_length;
          RETURN_NOT_OK(child_index_builder.Reserve(value_length));
          for (offset_type j = value_offset; j < value_offset + value_length; ++j) {
            child_index_builder.UnsafeAppend(j);
          }
          return Status::OK();
        },
        [&]() {
          offset_builder.UnsafeAppend(offset);
          return Status::OK();
        }));
    offset_builder.UnsafeAppend(offset);
    return Status::OK();
  }

  Status Init() override {
    RETURN_NOT_OK(offset_builder.Reserve(output_length + 1));
    return Status::OK();
  }

  Status Finish() override {
    std::shared_ptr<Array> child_indices;
    RETURN_NOT_OK(child_index_builder.Finish(&child_indices));

    ValuesArrayType typed_values(this->values);

    // No need to boundscheck the child values indices
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> taken_child,
                          Take(*typed_values.values(), *child_indices,
                               TakeOptions::NoBoundsCheck(), ctx->exec_context()));
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    out->child_data = {taken_child->data()};
    return Status::OK();
  }
};

struct DenseUnionImpl : public Selection<DenseUnionImpl, DenseUnionType> {
  using Base = Selection<DenseUnionImpl, DenseUnionType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<int32_t> value_offset_buffer_builder_;
  TypedBufferBuilder<int8_t> child_id_buffer_builder_;
  std::vector<int8_t> type_codes_;
  std::vector<Int32Builder> child_indices_builders_;

  DenseUnionImpl(KernelContext* ctx, const ExecBatch& batch, int64_t output_length,
                 Datum* out)
      : Base(ctx, batch, output_length, out),
        value_offset_buffer_builder_(ctx->memory_pool()),
        child_id_buffer_builder_(ctx->memory_pool()),
        type_codes_(checked_cast<const UnionType&>(*this->values->type).type_codes()),
        child_indices_builders_(type_codes_.size()) {
    for (auto& child_indices_builder : child_indices_builders_) {
      child_indices_builder = Int32Builder(ctx->memory_pool());
    }
  }

  template <typename Adapter>
  Status GenerateOutput() {
    DenseUnionArray typed_values(this->values);
    Adapter adapter(this);
    RETURN_NOT_OK(adapter.Generate(
        [&](int64_t index) {
          int8_t child_id = typed_values.child_id(index);
          child_id_buffer_builder_.UnsafeAppend(type_codes_[child_id]);
          int32_t value_offset = typed_values.value_offset(index);
          value_offset_buffer_builder_.UnsafeAppend(
              static_cast<int32_t>(child_indices_builders_[child_id].length()));
          RETURN_NOT_OK(child_indices_builders_[child_id].Reserve(1));
          child_indices_builders_[child_id].UnsafeAppend(value_offset);
          return Status::OK();
        },
        [&]() {
          int8_t child_id = 0;
          child_id_buffer_builder_.UnsafeAppend(type_codes_[child_id]);
          value_offset_buffer_builder_.UnsafeAppend(
              static_cast<int32_t>(child_indices_builders_[child_id].length()));
          RETURN_NOT_OK(child_indices_builders_[child_id].Reserve(1));
          child_indices_builders_[child_id].UnsafeAppendNull();
          return Status::OK();
        }));
    return Status::OK();
  }

  Status Init() override {
    RETURN_NOT_OK(child_id_buffer_builder_.Reserve(output_length));
    RETURN_NOT_OK(value_offset_buffer_builder_.Reserve(output_length));
    return Status::OK();
  }

  Status Finish() override {
    ARROW_ASSIGN_OR_RAISE(auto child_ids_buffer, child_id_buffer_builder_.Finish());
    ARROW_ASSIGN_OR_RAISE(auto value_offsets_buffer,
                          value_offset_buffer_builder_.Finish());
    DenseUnionArray typed_values(this->values);
    auto num_fields = typed_values.num_fields();
    auto num_rows = child_ids_buffer->size();
    BufferVector buffers{nullptr, std::move(child_ids_buffer),
                         std::move(value_offsets_buffer)};
    *out = ArrayData(typed_values.type(), num_rows, std::move(buffers), 0);
    for (auto i = 0; i < num_fields; i++) {
      ARROW_ASSIGN_OR_RAISE(auto child_indices_array,
                            child_indices_builders_[i].Finish());
      ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> child_array,
                            Take(*typed_values.field(i), *child_indices_array));
      out->child_data.push_back(child_array->data());
    }
    return Status::OK();
  }
};

struct FSLImpl : public Selection<FSLImpl, FixedSizeListType> {
  Int64Builder child_index_builder;

  using Base = Selection<FSLImpl, FixedSizeListType>;
  LIFT_BASE_MEMBERS();

  FSLImpl(KernelContext* ctx, const ExecBatch& batch, int64_t output_length, Datum* out)
      : Base(ctx, batch, output_length, out), child_index_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    ValuesArrayType typed_values(this->values);
    int32_t list_size = typed_values.list_type()->list_size();

    /// We must take list_size elements even for null elements of
    /// indices.
    RETURN_NOT_OK(child_index_builder.Reserve(output_length * list_size));

    Adapter adapter(this);
    return adapter.Generate(
        [&](int64_t index) {
          int64_t offset = index * list_size;
          for (int64_t j = offset; j < offset + list_size; ++j) {
            child_index_builder.UnsafeAppend(j);
          }
          return Status::OK();
        },
        [&]() { return child_index_builder.AppendNulls(list_size); });
  }

  Status Finish() override {
    std::shared_ptr<Array> child_indices;
    RETURN_NOT_OK(child_index_builder.Finish(&child_indices));

    ValuesArrayType typed_values(this->values);

    // No need to boundscheck the child values indices
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> taken_child,
                          Take(*typed_values.values(), *child_indices,
                               TakeOptions::NoBoundsCheck(), ctx->exec_context()));
    out->child_data = {taken_child->data()};
    return Status::OK();
  }
};

// ----------------------------------------------------------------------
// Struct selection implementations

// We need a slightly different approach for StructType. For Take, we can
// invoke Take on each struct field's data with boundschecking disabled. For
// Filter on the other hand, if we naively call Filter on each field, then the
// filter output length will have to be redundantly computed. Thus, for Filter
// we instead convert the filter to selection indices and then invoke take.

// Struct selection implementation. ONLY used for Take
struct StructImpl : public Selection<StructImpl, StructType> {
  using Base = Selection<StructImpl, StructType>;
  LIFT_BASE_MEMBERS();
  using Base::Base;

  template <typename Adapter>
  Status GenerateOutput() {
    StructArray typed_values(values);
    Adapter adapter(this);
    // There's nothing to do for Struct except to generate the validity bitmap
    return adapter.Generate([&](int64_t index) { return Status::OK(); },
                            /*visit_null=*/VisitNoop);
  }

  Status Finish() override {
    StructArray typed_values(values);

    // Select from children without boundschecking
    out->child_data.resize(values->type->num_fields());
    for (int field_index = 0; field_index < values->type->num_fields(); ++field_index) {
      ARROW_ASSIGN_OR_RAISE(Datum taken_field,
                            Take(Datum(typed_values.field(field_index)), Datum(selection),
                                 TakeOptions::NoBoundsCheck(), ctx->exec_context()));
      out->child_data[field_index] = taken_field.array();
    }
    return Status::OK();
  }
};

Status StructFilter(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // Transform filter to selection indices and then use Take.
  std::shared_ptr<ArrayData> indices;
  RETURN_NOT_OK(GetTakeIndices(*batch[1].array(),
                               FilterState::Get(ctx).null_selection_behavior,
                               ctx->memory_pool())
                    .Value(&indices));

  Datum result;
  RETURN_NOT_OK(
      Take(batch[0], Datum(indices), TakeOptions::NoBoundsCheck(), ctx->exec_context())
          .Value(&result));
  out->value = result.array();
  return Status::OK();
}

#undef LIFT_BASE_MEMBERS

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

static auto kDefaultFilterOptions = FilterOptions::Defaults();

const FunctionDoc filter_doc(
    "Filter with a boolean selection filter",
    ("The output is populated with values from the input at positions\n"
     "where the selection filter is non-zero.  Nulls in the selection filter\n"
     "are handled based on FilterOptions."),
    {"input", "selection_filter"}, "FilterOptions");

class FilterMetaFunction : public MetaFunction {
 public:
  FilterMetaFunction()
      : MetaFunction("filter", Arity::Binary(), &filter_doc, &kDefaultFilterOptions) {}

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
// Implement Take metafunction

// Shorthand naming of these functions
// A -> Array
// C -> ChunkedArray
// R -> RecordBatch
// T -> Table

Result<std::shared_ptr<Array>> TakeAA(const Array& values, const Array& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        CallFunction("array_take", {values, indices}, &options, ctx));
  return result.make_array();
}

Result<std::shared_ptr<ChunkedArray>> TakeCA(const ChunkedArray& values,
                                             const Array& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = values.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(1);  // Hard-coded 1 for now
  std::shared_ptr<Array> current_chunk;

  // Case 1: `values` has a single chunk, so just use it
  if (num_chunks == 1) {
    current_chunk = values.chunk(0);
  } else {
    // TODO Case 2: See if all `indices` fall in the same chunk and call Array Take on it
    // See
    // https://github.com/apache/arrow/blob/6f2c9041137001f7a9212f244b51bc004efc29af/r/src/compute.cpp#L123-L151
    // TODO Case 3: If indices are sorted, can slice them and call Array Take

    // Case 4: Else, concatenate chunks and call Array Take
    ARROW_ASSIGN_OR_RAISE(current_chunk,
                          Concatenate(values.chunks(), ctx->memory_pool()));
  }
  // Call Array Take on our single chunk
  ARROW_ASSIGN_OR_RAISE(new_chunks[0], TakeAA(*current_chunk, indices, options, ctx));
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> TakeCC(const ChunkedArray& values,
                                             const ChunkedArray& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    // Note that as currently implemented, this is inefficient because `values`
    // will get concatenated on every iteration of this loop
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ChunkedArray> current_chunk,
                          TakeCA(values, *indices.chunk(i), options, ctx));
    // Concatenate the result to make a single array for this chunk
    ARROW_ASSIGN_OR_RAISE(new_chunks[i],
                          Concatenate(current_chunk->chunks(), ctx->memory_pool()));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<ChunkedArray>> TakeAC(const Array& values,
                                             const ChunkedArray& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    ARROW_ASSIGN_OR_RAISE(new_chunks[i], TakeAA(values, *indices.chunk(i), options, ctx));
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks));
}

Result<std::shared_ptr<RecordBatch>> TakeRA(const RecordBatch& batch,
                                            const Array& indices,
                                            const TakeOptions& options,
                                            ExecContext* ctx) {
  auto ncols = batch.num_columns();
  auto nrows = indices.length();
  std::vector<std::shared_ptr<Array>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeAA(*batch.column(j), indices, options, ctx));
  }
  return RecordBatch::Make(batch.schema(), nrows, std::move(columns));
}

Result<std::shared_ptr<Table>> TakeTA(const Table& table, const Array& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCA(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), std::move(columns));
}

Result<std::shared_ptr<Table>> TakeTC(const Table& table, const ChunkedArray& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCC(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), std::move(columns));
}

static auto kDefaultTakeOptions = TakeOptions::Defaults();

const FunctionDoc take_doc(
    "Select values from an input based on indices from another array",
    ("The output is populated with values from the input at positions\n"
     "given by `indices`.  Nulls in `indices` emit null in the output."),
    {"input", "indices"}, "TakeOptions");

// Metafunction for dispatching to different Take implementations other than
// Array-Array.
//
// TODO: Revamp approach to executing Take operations. In addition to being
// overly complex dispatching, there is no parallelization.
class TakeMetaFunction : public MetaFunction {
 public:
  TakeMetaFunction()
      : MetaFunction("take", Arity::Binary(), &take_doc, &kDefaultTakeOptions) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    Datum::Kind index_kind = args[1].kind();
    const TakeOptions& take_opts = static_cast<const TakeOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeAA(*args[0].make_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeAC(*args[0].make_array(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      case Datum::CHUNKED_ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeCA(*args[0].chunked_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeCC(*args[0].chunked_array(), *args[1].chunked_array(), take_opts,
                        ctx);
        }
        break;
      case Datum::RECORD_BATCH:
        if (index_kind == Datum::ARRAY) {
          return TakeRA(*args[0].record_batch(), *args[1].make_array(), take_opts, ctx);
        }
        break;
      case Datum::TABLE:
        if (index_kind == Datum::ARRAY) {
          return TakeTA(*args[0].table(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeTC(*args[0].table(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      default:
        break;
    }
    return Status::NotImplemented(
        "Unsupported types for take operation: "
        "values=",
        args[0].ToString(), "indices=", args[1].ToString());
  }
};

// ----------------------------------------------------------------------

template <typename Impl>
Status FilterExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // TODO: where are the values and filter length equality checked?
  int64_t output_length = GetFilterOutputSize(
      *batch[1].array(), FilterState::Get(ctx).null_selection_behavior);
  Impl kernel(ctx, batch, output_length, out);
  return kernel.ExecFilter();
}

template <typename Impl>
Status TakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(*batch[1].array(), batch[0].length()));
  }
  Impl kernel(ctx, batch, /*output_length=*/batch[1].length(), out);
  return kernel.ExecTake();
}

struct SelectionKernelDescr {
  InputType input;
  ArrayKernelExec exec;
};

void RegisterSelectionFunction(const std::string& name, const FunctionDoc* doc,
                               VectorKernel base_kernel, InputType selection_type,
                               const std::vector<SelectionKernelDescr>& descrs,
                               const FunctionOptions* default_options,
                               FunctionRegistry* registry) {
  auto func =
      std::make_shared<VectorFunction>(name, Arity::Binary(), doc, default_options);
  for (auto& descr : descrs) {
    base_kernel.signature = KernelSignature::Make(
        {std::move(descr.input), selection_type}, OutputType(FirstType));
    base_kernel.exec = descr.exec;
    DCHECK_OK(func->AddKernel(base_kernel));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

const FunctionDoc array_filter_doc(
    "Filter with a boolean selection filter",
    ("The output is populated with values from the input `array` at positions\n"
     "where the selection filter is non-zero.  Nulls in the selection filter\n"
     "are handled based on FilterOptions."),
    {"array", "selection_filter"}, "FilterOptions");

const FunctionDoc array_take_doc(
    "Select values from an array based on indices from another array",
    ("The output is populated with values from the input array at positions\n"
     "given by `indices`.  Nulls in `indices` emit null in the output."),
    {"array", "indices"}, "TakeOptions");

}  // namespace

void RegisterVectorSelection(FunctionRegistry* registry) {
  // Filter kernels
  std::vector<SelectionKernelDescr> filter_kernel_descrs = {
      {InputType(match::Primitive(), ValueDescr::ARRAY), PrimitiveFilter},
      {InputType(match::BinaryLike(), ValueDescr::ARRAY), BinaryFilter},
      {InputType(match::LargeBinaryLike(), ValueDescr::ARRAY), BinaryFilter},
      {InputType::Array(Type::FIXED_SIZE_BINARY), FilterExec<FSBImpl>},
      {InputType::Array(null()), NullFilter},
      {InputType::Array(Type::DECIMAL), FilterExec<FSBImpl>},
      {InputType::Array(Type::DICTIONARY), DictionaryFilter},
      {InputType::Array(Type::EXTENSION), ExtensionFilter},
      {InputType::Array(Type::LIST), FilterExec<ListImpl<ListType>>},
      {InputType::Array(Type::LARGE_LIST), FilterExec<ListImpl<LargeListType>>},
      {InputType::Array(Type::FIXED_SIZE_LIST), FilterExec<FSLImpl>},
      {InputType::Array(Type::DENSE_UNION), FilterExec<DenseUnionImpl>},
      {InputType::Array(Type::STRUCT), StructFilter},
      // TODO: Reuse ListType kernel for MAP
      {InputType::Array(Type::MAP), FilterExec<ListImpl<MapType>>},
  };

  VectorKernel filter_base;
  filter_base.init = FilterState::Init;
  RegisterSelectionFunction("array_filter", &array_filter_doc, filter_base,
                            /*selection_type=*/InputType::Array(boolean()),
                            filter_kernel_descrs, &kDefaultFilterOptions, registry);

  DCHECK_OK(registry->AddFunction(std::make_shared<FilterMetaFunction>()));

  // Take kernels
  std::vector<SelectionKernelDescr> take_kernel_descrs = {
      {InputType(match::Primitive(), ValueDescr::ARRAY), PrimitiveTake},
      {InputType(match::BinaryLike(), ValueDescr::ARRAY),
       TakeExec<VarBinaryImpl<BinaryType>>},
      {InputType(match::LargeBinaryLike(), ValueDescr::ARRAY),
       TakeExec<VarBinaryImpl<LargeBinaryType>>},
      {InputType::Array(Type::FIXED_SIZE_BINARY), TakeExec<FSBImpl>},
      {InputType::Array(null()), NullTake},
      {InputType::Array(Type::DECIMAL128), TakeExec<FSBImpl>},
      {InputType::Array(Type::DECIMAL256), TakeExec<FSBImpl>},
      {InputType::Array(Type::DICTIONARY), DictionaryTake},
      {InputType::Array(Type::EXTENSION), ExtensionTake},
      {InputType::Array(Type::LIST), TakeExec<ListImpl<ListType>>},
      {InputType::Array(Type::LARGE_LIST), TakeExec<ListImpl<LargeListType>>},
      {InputType::Array(Type::FIXED_SIZE_LIST), TakeExec<FSLImpl>},
      {InputType::Array(Type::DENSE_UNION), TakeExec<DenseUnionImpl>},
      {InputType::Array(Type::STRUCT), TakeExec<StructImpl>},
      // TODO: Reuse ListType kernel for MAP
      {InputType::Array(Type::MAP), TakeExec<ListImpl<MapType>>},
  };

  VectorKernel take_base;
  take_base.init = TakeState::Init;
  take_base.can_execute_chunkwise = false;
  RegisterSelectionFunction(
      "array_take", &array_take_doc, take_base,
      /*selection_type=*/InputType(match::Integer(), ValueDescr::ARRAY),
      take_kernel_descrs, &kDefaultTakeOptions, registry);

  DCHECK_OK(registry->AddFunction(std::make_shared<TakeMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
