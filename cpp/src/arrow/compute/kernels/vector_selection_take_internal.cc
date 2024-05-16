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
#include <limits>
#include <memory>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/compute/kernels/vector_selection_take_internal.h"
#include "arrow/memory_pool.h"
#include "arrow/record_batch.h"
#include "arrow/table.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/fixed_width_internal.h"
#include "arrow/util/int_util.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CheckIndexBounds;
using internal::OptionalBitBlockCounter;

namespace compute {
namespace internal {

namespace {

template <typename IndexType>
Result<std::shared_ptr<ArrayData>> GetTakeIndicesFromBitmapImpl(
    const ArraySpan& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  using T = typename IndexType::c_type;

  const uint8_t* filter_data = filter.buffers[1].data;
  const bool have_filter_nulls = filter.MayHaveNulls();
  const uint8_t* filter_is_valid = filter.buffers[0].data;

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
          if (bit_util::GetBit(filter_is_valid, position_with_offset)) {
            if (bit_util::GetBit(filter_data, position_with_offset)) {
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
          if (bit_util::GetBit(filter_is_valid, position_with_offset) &&
              bit_util::GetBit(filter_data, position_with_offset)) {
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

template <typename RunEndType>
Result<std::shared_ptr<ArrayData>> GetTakeIndicesFromREEBitmapImpl(
    const ArraySpan& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  using T = typename RunEndType::c_type;
  const ArraySpan& filter_values = ::arrow::ree_util::ValuesArray(filter);
  const int64_t filter_values_offset = filter_values.offset;
  const uint8_t* filter_is_valid = filter_values.buffers[0].data;
  const uint8_t* filter_selection = filter_values.buffers[1].data;
  const bool filter_may_have_nulls = filter_values.MayHaveNulls();

  // BinaryBitBlockCounter is not used here because a REE bitmap, if built
  // correctly, is not going to have long continuous runs of 0s or 1s in the
  // values array.

  const ::arrow::ree_util::RunEndEncodedArraySpan<T> filter_span(filter);
  auto it = filter_span.begin();
  if (filter_may_have_nulls && null_selection == FilterOptions::EMIT_NULL) {
    // Most complex case: the filter may have nulls and we don't drop them.
    // The logic is ternary:
    // - filter is null: emit null
    // - filter is valid and true: emit index
    // - filter is valid and false: don't emit anything

    typename TypeTraits<RunEndType>::BuilderType builder(memory_pool);
    for (; !it.is_end(filter_span); ++it) {
      const int64_t position_with_offset = filter_values_offset + it.index_into_array();
      const bool is_null = !bit_util::GetBit(filter_is_valid, position_with_offset);
      if (is_null) {
        RETURN_NOT_OK(builder.AppendNulls(it.run_length()));
      } else {
        const bool emit_run = bit_util::GetBit(filter_selection, position_with_offset);
        if (emit_run) {
          const int64_t run_end = it.run_end();
          RETURN_NOT_OK(builder.Reserve(run_end - it.logical_position()));
          for (int64_t position = it.logical_position(); position < run_end; position++) {
            builder.UnsafeAppend(static_cast<T>(position));
          }
        }
      }
    }
    std::shared_ptr<ArrayData> result;
    RETURN_NOT_OK(builder.FinishInternal(&result));
    return result;
  }

  // Other cases don't emit nulls and are therefore simpler.
  TypedBufferBuilder<T> builder(memory_pool);

  if (filter_may_have_nulls) {
    DCHECK_EQ(null_selection, FilterOptions::DROP);
    // The filter may have nulls, so we scan the validity bitmap and the filter
    // data bitmap together.
    for (; !it.is_end(filter_span); ++it) {
      const int64_t position_with_offset = filter_values_offset + it.index_into_array();
      const bool emit_run = bit_util::GetBit(filter_is_valid, position_with_offset) &&
                            bit_util::GetBit(filter_selection, position_with_offset);
      if (emit_run) {
        const int64_t run_end = it.run_end();
        RETURN_NOT_OK(builder.Reserve(run_end - it.logical_position()));
        for (int64_t position = it.logical_position(); position < run_end; position++) {
          builder.UnsafeAppend(static_cast<T>(position));
        }
      }
    }
  } else {
    // The filter has no nulls, so we need only look for true values
    for (; !it.is_end(filter_span); ++it) {
      const int64_t position_with_offset = filter_values_offset + it.index_into_array();
      const bool emit_run = bit_util::GetBit(filter_selection, position_with_offset);
      if (emit_run) {
        const int64_t run_end = it.run_end();
        RETURN_NOT_OK(builder.Reserve(run_end - it.logical_position()));
        for (int64_t position = it.logical_position(); position < run_end; position++) {
          builder.UnsafeAppend(static_cast<T>(position));
        }
      }
    }
  }

  const int64_t length = builder.length();
  std::shared_ptr<Buffer> out_buffer;
  RETURN_NOT_OK(builder.Finish(&out_buffer));
  return std::make_shared<ArrayData>(TypeTraits<RunEndType>::type_singleton(), length,
                                     BufferVector{nullptr, std::move(out_buffer)},
                                     /*null_count=*/0);
}

Result<std::shared_ptr<ArrayData>> GetTakeIndicesFromBitmap(
    const ArraySpan& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  DCHECK_EQ(filter.type->id(), Type::BOOL);
  if (filter.length <= std::numeric_limits<uint16_t>::max()) {
    return GetTakeIndicesFromBitmapImpl<UInt16Type>(filter, null_selection, memory_pool);
  } else if (filter.length <= std::numeric_limits<uint32_t>::max()) {
    return GetTakeIndicesFromBitmapImpl<UInt32Type>(filter, null_selection, memory_pool);
  } else {
    // Arrays over 4 billion elements, not especially likely.
    return Status::NotImplemented(
        "Filter length exceeds UINT32_MAX, "
        "consider a different strategy for selecting elements");
  }
}

Result<std::shared_ptr<ArrayData>> GetTakeIndicesFromREEBitmap(
    const ArraySpan& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  const auto& ree_type = checked_cast<const RunEndEncodedType&>(*filter.type);
  // The resulting array will contain indexes of the same type as the run-end type of the
  // run-end encoded filter. Run-end encoded arrays have to pick the smallest run-end type
  // to maximize memory savings, so we can be re-use that decision here and get a good
  // result without checking the logical length of the filter.
  switch (ree_type.run_end_type()->id()) {
    case Type::INT16:
      return GetTakeIndicesFromREEBitmapImpl<Int16Type>(filter, null_selection,
                                                        memory_pool);
    case Type::INT32:
      return GetTakeIndicesFromREEBitmapImpl<Int32Type>(filter, null_selection,
                                                        memory_pool);
    default:
      DCHECK_EQ(ree_type.run_end_type()->id(), Type::INT64);
      return GetTakeIndicesFromREEBitmapImpl<Int64Type>(filter, null_selection,
                                                        memory_pool);
  }
}

}  // namespace

Result<std::shared_ptr<ArrayData>> GetTakeIndices(
    const ArraySpan& filter, FilterOptions::NullSelectionBehavior null_selection,
    MemoryPool* memory_pool) {
  if (filter.type->id() == Type::BOOL) {
    return GetTakeIndicesFromBitmap(filter, null_selection, memory_pool);
  }
  return GetTakeIndicesFromREEBitmap(filter, null_selection, memory_pool);
}

namespace {

using TakeState = OptionsWrapper<TakeOptions>;

// ----------------------------------------------------------------------
// Implement optimized take for primitive types from boolean to 1/2/4/8/16/32-byte
// C-type based types. Use common implementation for every byte width and only
// generate code for unsigned integer indices, since after boundschecking to
// check for negative numbers in the indices we can safely reinterpret_cast
// signed integers as unsigned.

/// \brief The Take implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather the physical C type. This way we
/// only generate one take function for each byte width.
///
/// Also note that this function can also handle fixed-size-list arrays if
/// they fit the criteria described in fixed_width_internal.h, so use the
/// function defined in that file to access values and destination pointers
/// and DO NOT ASSUME `values.type()` is a primitive type.
///
/// \pre the indices have been boundschecked
template <typename IndexCType, typename ValueWidthConstant>
struct PrimitiveTakeImpl {
  static constexpr int kValueWidth = ValueWidthConstant::value;

  static void Exec(const ArraySpan& values, const ArraySpan& indices,
                   ArrayData* out_arr) {
    DCHECK_EQ(util::FixedWidthInBytes(*values.type), kValueWidth);
    const auto* values_data = util::OffsetPointerOfFixedByteWidthValues(values);
    const uint8_t* values_is_valid = values.buffers[0].data;
    auto values_offset = values.offset;

    const auto* indices_data = indices.GetValues<IndexCType>(1);
    const uint8_t* indices_is_valid = indices.buffers[0].data;
    auto indices_offset = indices.offset;

    DCHECK_EQ(out_arr->offset, 0);
    auto* out = util::MutableFixedWidthValuesPointer(out_arr);
    auto out_is_valid = out_arr->buffers[0]->mutable_data();

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (values.null_count != 0 || indices.null_count != 0) {
      bit_util::SetBitsTo(out_is_valid, 0, indices.length, false);
    }

    auto WriteValue = [&](int64_t position) {
      memcpy(out + position * kValueWidth,
             values_data + indices_data[position] * kValueWidth, kValueWidth);
    };

    auto WriteZero = [&](int64_t position) {
      memset(out + position * kValueWidth, 0, kValueWidth);
    };

    auto WriteZeroSegment = [&](int64_t position, int64_t length) {
      memset(out + position * kValueWidth, 0, kValueWidth * length);
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
          bit_util::SetBitsTo(out_is_valid, position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            WriteValue(position);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (bit_util::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, position);
              WriteValue(position);
            } else {
              WriteZero(position);
            }
            ++position;
          }
        } else {
          WriteZeroSegment(position, block.length);
          position += block.length;
        }
      } else {
        // Values have nulls, so we must do random access into the values bitmap
        if (block.popcount == block.length) {
          // Faster path: indices are not null but values may be
          for (int64_t i = 0; i < block.length; ++i) {
            if (bit_util::GetBit(values_is_valid,
                                 values_offset + indices_data[position])) {
              // value is not null
              WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else {
              WriteZero(position);
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            if (bit_util::GetBit(indices_is_valid, indices_offset + position) &&
                bit_util::GetBit(values_is_valid,
                                 values_offset + indices_data[position])) {
              // index is not null && value is not null
              WriteValue(position);
              bit_util::SetBit(out_is_valid, position);
              ++valid_count;
            } else {
              WriteZero(position);
            }
            ++position;
          }
        } else {
          WriteZeroSegment(position, block.length);
          position += block.length;
        }
      }
    }
    out_arr->null_count = out_arr->length - valid_count;
  }
};

template <typename IndexCType>
struct BooleanTakeImpl {
  static void Exec(const ArraySpan& values, const ArraySpan& indices,
                   ArrayData* out_arr) {
    const uint8_t* values_data = values.buffers[1].data;
    const uint8_t* values_is_valid = values.buffers[0].data;
    auto values_offset = values.offset;

    const auto* indices_data = indices.GetValues<IndexCType>(1);
    const uint8_t* indices_is_valid = indices.buffers[0].data;
    auto indices_offset = indices.offset;

    auto out = out_arr->buffers[1]->mutable_data();
    auto out_is_valid = out_arr->buffers[0]->mutable_data();
    auto out_offset = out_arr->offset;

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (values.null_count != 0 || indices.null_count != 0) {
      bit_util::SetBitsTo(out_is_valid, out_offset, indices.length, false);
    }
    // Avoid uninitialized data in values array
    bit_util::SetBitsTo(out, out_offset, indices.length, false);

    auto PlaceDataBit = [&](int64_t loc, IndexCType index) {
      bit_util::SetBitTo(out, out_offset + loc,
                         bit_util::GetBit(values_data, values_offset + index));
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
          bit_util::SetBitsTo(out_is_valid, out_offset + position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            PlaceDataBit(position, indices_data[position]);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (bit_util::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              bit_util::SetBit(out_is_valid, out_offset + position);
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
            if (bit_util::GetBit(values_is_valid,
                                 values_offset + indices_data[position])) {
              // value is not null
              bit_util::SetBit(out_is_valid, out_offset + position);
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
            if (bit_util::GetBit(indices_is_valid, indices_offset + position)) {
              // index is not null
              if (bit_util::GetBit(values_is_valid,
                                   values_offset + indices_data[position])) {
                // value is not null
                PlaceDataBit(position, indices_data[position]);
                bit_util::SetBit(out_is_valid, out_offset + position);
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
void TakeIndexDispatch(const ArraySpan& values, const ArraySpan& indices,
                       ArrayData* out) {
  // With the simplifying assumption that boundschecking has taken place
  // already at a higher level, we can now assume that the index values are all
  // non-negative. Thus, we can interpret signed integers as unsigned and avoid
  // having to generate double the amount of binary code to handle each integer
  // width.
  switch (indices.type->byte_width()) {
    case 1:
      return TakeImpl<uint8_t, Args...>::Exec(values, indices, out);
    case 2:
      return TakeImpl<uint16_t, Args...>::Exec(values, indices, out);
    case 4:
      return TakeImpl<uint32_t, Args...>::Exec(values, indices, out);
    case 8:
      return TakeImpl<uint64_t, Args...>::Exec(values, indices, out);
    default:
      DCHECK(false) << "Invalid indices byte width";
      break;
  }
}

}  // namespace

Status PrimitiveTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& values = batch[0].array;
  const ArraySpan& indices = batch[1].array;

  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(indices, values.length));
  }

  ArrayData* out_arr = out->array_data().get();

  DCHECK(util::IsFixedWidthLike(values));
  const int64_t bit_width = util::FixedWidthInBits(*values.type);

  // TODO: When neither values nor indices contain nulls, we can skip
  // allocating the validity bitmap altogether and save time and space. A
  // streamlined PrimitiveTakeImpl would need to be written that skips all
  // interactions with the output validity bitmap, though.
  RETURN_NOT_OK(util::internal::PreallocateFixedWidthArrayData(
      ctx, indices.length, /*source=*/values,
      /*allocate_validity=*/true, out_arr));
  switch (bit_width) {
    case 1:
      TakeIndexDispatch<BooleanTakeImpl>(values, indices, out_arr);
      break;
    case 8:
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 1>>(
          values, indices, out_arr);
      break;
    case 16:
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 2>>(
          values, indices, out_arr);
      break;
    case 32:
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 4>>(
          values, indices, out_arr);
      break;
    case 64:
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 8>>(
          values, indices, out_arr);
      break;
    case 128:
      // For INTERVAL_MONTH_DAY_NANO, DECIMAL128
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 16>>(
          values, indices, out_arr);
      break;
    case 256:
      // For DECIMAL256
      TakeIndexDispatch<PrimitiveTakeImpl, std::integral_constant<int, 32>>(
          values, indices, out_arr);
      break;
    default:
      return Status::NotImplemented("Unsupported primitive type for take: ",
                                    *values.type);
  }
  return Status::OK();
}

namespace {

// ----------------------------------------------------------------------
// Null take

Status NullTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(batch[1].array, batch[0].length()));
  }
  // batch.length doesn't take into account the take indices
  auto new_length = batch[1].array.length;
  out->value = std::make_shared<NullArray>(new_length)->data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Dictionary take

Status DictionaryTake(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  DictionaryArray values(batch[0].array.ToArrayData());
  Datum result;
  RETURN_NOT_OK(Take(Datum(values.indices()), batch[1].array.ToArrayData(),
                     TakeState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  DictionaryArray taken_values(values.type(), result.make_array(), values.dictionary());
  out->value = taken_values.data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Extension take

Status ExtensionTake(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ExtensionArray values(batch[0].array.ToArrayData());
  Datum result;
  RETURN_NOT_OK(Take(Datum(values.storage()), batch[1].array.ToArrayData(),
                     TakeState::Get(ctx), ctx->exec_context())
                    .Value(&result));
  ExtensionArray taken_values(values.type(), result.make_array());
  out->value = taken_values.data();
  return Status::OK();
}

// ----------------------------------------------------------------------
// Take metafunction implementation

// Shorthand naming of these functions
// A -> Array
// C -> ChunkedArray
// R -> RecordBatch
// T -> Table

Result<std::shared_ptr<ArrayData>> TakeAAA(const std::shared_ptr<ArrayData>& values,
                                           const std::shared_ptr<ArrayData>& indices,
                                           const TakeOptions& options, ExecContext* ctx) {
  ARROW_ASSIGN_OR_RAISE(Datum result,
                        CallFunction("array_take", {values, indices}, &options, ctx));
  return result.array();
}

Result<std::shared_ptr<ChunkedArray>> TakeCAC(const ChunkedArray& values,
                                              const Array& indices,
                                              const TakeOptions& options,
                                              ExecContext* ctx) {
  std::shared_ptr<Array> values_array;
  if (values.num_chunks() == 1) {
    // Case 1: `values` has a single chunk, so just use it
    values_array = values.chunk(0);
  } else {
    // TODO Case 2: See if all `indices` fall in the same chunk and call Array Take on it
    // See
    // https://github.com/apache/arrow/blob/6f2c9041137001f7a9212f244b51bc004efc29af/r/src/compute.cpp#L123-L151
    // TODO Case 3: If indices are sorted, can slice them and call Array Take
    // (these are relevant to TakeCCC as well)

    // Case 4: Else, concatenate chunks and call Array Take
    if (values.chunks().empty()) {
      ARROW_ASSIGN_OR_RAISE(
          values_array, MakeArrayOfNull(values.type(), /*length=*/0, ctx->memory_pool()));
    } else {
      ARROW_ASSIGN_OR_RAISE(values_array,
                            Concatenate(values.chunks(), ctx->memory_pool()));
    }
  }
  // Call Array Take on our single chunk
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> new_chunk,
                        TakeAAA(values_array->data(), indices.data(), options, ctx));
  std::vector<std::shared_ptr<Array>> chunks = {MakeArray(new_chunk)};
  return std::make_shared<ChunkedArray>(std::move(chunks));
}

Result<std::shared_ptr<ChunkedArray>> TakeCCC(const ChunkedArray& values,
                                              const ChunkedArray& indices,
                                              const TakeOptions& options,
                                              ExecContext* ctx) {
  // XXX: for every chunk in indices, values are gathered from all chunks in values to
  // form a new chunk in the result. Performing this concatenation is not ideal, but
  // greatly simplifies the implementation before something more efficient is
  // implemented.
  std::shared_ptr<Array> values_array;
  if (values.num_chunks() == 1) {
    values_array = values.chunk(0);
  } else {
    if (values.chunks().empty()) {
      ARROW_ASSIGN_OR_RAISE(
          values_array, MakeArrayOfNull(values.type(), /*length=*/0, ctx->memory_pool()));
    } else {
      ARROW_ASSIGN_OR_RAISE(values_array,
                            Concatenate(values.chunks(), ctx->memory_pool()));
    }
  }
  std::vector<std::shared_ptr<Array>> new_chunks;
  new_chunks.resize(indices.num_chunks());
  for (int i = 0; i < indices.num_chunks(); i++) {
    ARROW_ASSIGN_OR_RAISE(auto chunk, TakeAAA(values_array->data(),
                                              indices.chunk(i)->data(), options, ctx));
    new_chunks[i] = MakeArray(chunk);
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks), values.type());
}

Result<std::shared_ptr<ChunkedArray>> TakeACC(const Array& values,
                                              const ChunkedArray& indices,
                                              const TakeOptions& options,
                                              ExecContext* ctx) {
  auto num_chunks = indices.num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> chunk,
                          TakeAAA(values.data(), indices.chunk(i)->data(), options, ctx));
    new_chunks[i] = MakeArray(chunk);
  }
  return std::make_shared<ChunkedArray>(std::move(new_chunks), values.type());
}

Result<std::shared_ptr<RecordBatch>> TakeRAR(const RecordBatch& batch,
                                             const Array& indices,
                                             const TakeOptions& options,
                                             ExecContext* ctx) {
  auto ncols = batch.num_columns();
  auto nrows = indices.length();
  std::vector<std::shared_ptr<Array>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<ArrayData> col_data,
                          TakeAAA(batch.column(j)->data(), indices.data(), options, ctx));
    columns[j] = MakeArray(col_data);
  }
  return RecordBatch::Make(batch.schema(), nrows, std::move(columns));
}

Result<std::shared_ptr<Table>> TakeTAT(const Table& table, const Array& indices,
                                       const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCAC(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), std::move(columns));
}

Result<std::shared_ptr<Table>> TakeTCT(const Table& table, const ChunkedArray& indices,
                                       const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCCC(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), std::move(columns));
}

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
      : MetaFunction("take", Arity::Binary(), take_doc, GetDefaultTakeOptions()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    Datum::Kind index_kind = args[1].kind();
    const auto& take_opts = static_cast<const TakeOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeAAA(args[0].array(), args[1].array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeACC(*args[0].make_array(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      case Datum::CHUNKED_ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeCAC(*args[0].chunked_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeCCC(*args[0].chunked_array(), *args[1].chunked_array(), take_opts,
                         ctx);
        }
        break;
      case Datum::RECORD_BATCH:
        if (index_kind == Datum::ARRAY) {
          return TakeRAR(*args[0].record_batch(), *args[1].make_array(), take_opts, ctx);
        }
        break;
      case Datum::TABLE:
        if (index_kind == Datum::ARRAY) {
          return TakeTAT(*args[0].table(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeTCT(*args[0].table(), *args[1].chunked_array(), take_opts, ctx);
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

}  // namespace

const TakeOptions* GetDefaultTakeOptions() {
  static const auto kDefaultTakeOptions = TakeOptions::Defaults();
  return &kDefaultTakeOptions;
}

std::unique_ptr<Function> MakeTakeMetaFunction() {
  return std::make_unique<TakeMetaFunction>();
}

void PopulateTakeKernels(std::vector<SelectionKernelData>* out) {
  auto take_indices = match::Integer();

  *out = {
      {InputType(match::Primitive()), take_indices, PrimitiveTakeExec},
      {InputType(match::BinaryLike()), take_indices, VarBinaryTakeExec},
      {InputType(match::LargeBinaryLike()), take_indices, LargeVarBinaryTakeExec},
      {InputType(Type::FIXED_SIZE_BINARY), take_indices, FSBTakeExec},
      {InputType(null()), take_indices, NullTakeExec},
      {InputType(Type::DECIMAL128), take_indices, PrimitiveTakeExec},
      {InputType(Type::DECIMAL256), take_indices, PrimitiveTakeExec},
      {InputType(Type::DICTIONARY), take_indices, DictionaryTake},
      {InputType(Type::EXTENSION), take_indices, ExtensionTake},
      {InputType(Type::LIST), take_indices, ListTakeExec},
      {InputType(Type::LARGE_LIST), take_indices, LargeListTakeExec},
      {InputType(Type::FIXED_SIZE_LIST), take_indices, FSLTakeExec},
      {InputType(Type::DENSE_UNION), take_indices, DenseUnionTakeExec},
      {InputType(Type::SPARSE_UNION), take_indices, SparseUnionTakeExec},
      {InputType(Type::STRUCT), take_indices, StructTakeExec},
      {InputType(Type::MAP), take_indices, MapTakeExec},
  };
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
