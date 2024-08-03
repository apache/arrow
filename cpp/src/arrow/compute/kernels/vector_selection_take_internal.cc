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
#include <utility>
#include <vector>

#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/gather_internal.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/compute/kernels/vector_selection_take_internal.h"
#include "arrow/compute/registry.h"
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
// Implement optimized take for primitive types from boolean to
// 1/2/4/8/16/32-byte C-type based types and fixed-size binary (0 or more
// bytes).
//
// Use one specialization for each of these primitive byte-widths so the
// compiler can specialize the memcpy to dedicated CPU instructions and for
// fixed-width binary use the 1-byte specialization but pass WithFactor=true
// that makes the kernel consider the factor parameter provided at runtime.
//
// Only unsigned index types need to be instantiated since after
// boundschecking to check for negative numbers in the indices we can safely
// reinterpret_cast signed integers as unsigned.

/// \brief The Take implementation for primitive types and fixed-width binary.
///
/// Also note that this function can also handle fixed-size-list arrays if
/// they fit the criteria described in fixed_width_internal.h, so use the
/// function defined in that file to access values and destination pointers
/// and DO NOT ASSUME `values.type()` is a primitive type.
///
/// NOTE: Template parameters are types instead of values to let
/// `TakeIndexDispatch<>` forward `typename... Args`  after the index type.
///
/// \pre the indices have been boundschecked
template <typename IndexCType, typename ValueBitWidthConstant,
          typename OutputIsZeroInitialized = std::false_type,
          typename WithFactor = std::false_type>
struct FixedWidthTakeImpl {
  static constexpr int kValueWidthInBits = ValueBitWidthConstant::value;

  static Status Exec(KernelContext* ctx, const ArraySpan& values,
                     const ArraySpan& indices, ArrayData* out_arr, int64_t factor) {
#ifndef NDEBUG
    int64_t bit_width = util::FixedWidthInBits(*values.type);
    DCHECK(WithFactor::value || (kValueWidthInBits == bit_width && factor == 1));
    DCHECK(!WithFactor::value ||
           (factor > 0 && kValueWidthInBits == 8 &&  // factors are used with bytes
            static_cast<int64_t>(factor * kValueWidthInBits) == bit_width));
#endif
    const bool out_has_validity = values.MayHaveNulls() || indices.MayHaveNulls();

    const uint8_t* src;
    int64_t src_offset;
    std::tie(src_offset, src) = util::OffsetPointerOfFixedBitWidthValues(values);
    uint8_t* out = util::MutableFixedWidthValuesPointer(out_arr);
    int64_t valid_count = 0;
    arrow::internal::Gather<kValueWidthInBits, IndexCType, WithFactor::value> gather{
        /*src_length=*/values.length,
        src,
        src_offset,
        /*idx_length=*/indices.length,
        /*idx=*/indices.GetValues<IndexCType>(1),
        out,
        factor};
    if (out_has_validity) {
      DCHECK_EQ(out_arr->offset, 0);
      // out_is_valid must be zero-initiliazed, because Gather::Execute
      // saves time by not having to ClearBit on every null element.
      auto out_is_valid = out_arr->GetMutableValues<uint8_t>(0);
      memset(out_is_valid, 0, bit_util::BytesForBits(out_arr->length));
      valid_count = gather.template Execute<OutputIsZeroInitialized::value>(
          /*src_validity=*/values, /*idx_validity=*/indices, out_is_valid);
    } else {
      valid_count = gather.Execute();
    }
    out_arr->null_count = out_arr->length - valid_count;
    return Status::OK();
  }
};

template <template <typename...> class TakeImpl, typename... Args>
Status TakeIndexDispatch(KernelContext* ctx, const ArraySpan& values,
                         const ArraySpan& indices, ArrayData* out, int64_t factor = 1) {
  // With the simplifying assumption that boundschecking has taken place
  // already at a higher level, we can now assume that the index values are all
  // non-negative. Thus, we can interpret signed integers as unsigned and avoid
  // having to generate double the amount of binary code to handle each integer
  // width.
  switch (indices.type->byte_width()) {
    case 1:
      return TakeImpl<uint8_t, Args...>::Exec(ctx, values, indices, out, factor);
    case 2:
      return TakeImpl<uint16_t, Args...>::Exec(ctx, values, indices, out, factor);
    case 4:
      return TakeImpl<uint32_t, Args...>::Exec(ctx, values, indices, out, factor);
    default:
      DCHECK_EQ(indices.type->byte_width(), 8);
      return TakeImpl<uint64_t, Args...>::Exec(ctx, values, indices, out, factor);
  }
}

}  // namespace

Status FixedWidthTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const ArraySpan& values = batch[0].array;
  const ArraySpan& indices = batch[1].array;

  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(indices, values.length));
  }

  ArrayData* out_arr = out->array_data().get();
  DCHECK(util::IsFixedWidthLike(values));
  // When we know for sure that values nor indices contain nulls, we can skip
  // allocating the validity bitmap altogether and save time and space.
  const bool allocate_validity = values.MayHaveNulls() || indices.MayHaveNulls();
  RETURN_NOT_OK(util::internal::PreallocateFixedWidthArrayData(
      ctx, indices.length, /*source=*/values, allocate_validity, out_arr));
  switch (util::FixedWidthInBits(*values.type)) {
    case 0:
      DCHECK(values.type->id() == Type::FIXED_SIZE_BINARY ||
             values.type->id() == Type::FIXED_SIZE_LIST);
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 0>>(
          ctx, values, indices, out_arr);
    case 1:
      // Zero-initialize the data buffer for the output array when the bit-width is 1
      // (e.g. Boolean array) to avoid having to ClearBit on every null element.
      // This might be profitable for other types as well, but we take the most
      // conservative approach for now.
      memset(out_arr->buffers[1]->mutable_data(), 0, out_arr->buffers[1]->size());
      return TakeIndexDispatch<
          FixedWidthTakeImpl, std::integral_constant<int, 1>, /*OutputIsZeroInitialized=*/
          std::true_type>(ctx, values, indices, out_arr);
    case 8:
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 8>>(
          ctx, values, indices, out_arr);
    case 16:
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 16>>(
          ctx, values, indices, out_arr);
    case 32:
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 32>>(
          ctx, values, indices, out_arr);
    case 64:
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 64>>(
          ctx, values, indices, out_arr);
    case 128:
      // For INTERVAL_MONTH_DAY_NANO, DECIMAL128
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 128>>(
          ctx, values, indices, out_arr);
    case 256:
      // For DECIMAL256
      return TakeIndexDispatch<FixedWidthTakeImpl, std::integral_constant<int, 256>>(
          ctx, values, indices, out_arr);
  }
  if (ARROW_PREDICT_TRUE(values.type->id() == Type::FIXED_SIZE_BINARY ||
                         values.type->id() == Type::FIXED_SIZE_LIST)) {
    int64_t byte_width = util::FixedWidthInBytes(*values.type);
    // 0-length fixed-size binary or lists were handled above on `case 0`
    DCHECK_GT(byte_width, 0);
    return TakeIndexDispatch<FixedWidthTakeImpl,
                             /*ValueBitWidth=*/std::integral_constant<int, 8>,
                             /*OutputIsZeroInitialized=*/std::false_type,
                             /*WithFactor=*/std::true_type>(ctx, values, indices, out_arr,
                                                            /*factor=*/byte_width);
  }
  return Status::NotImplemented("Unsupported primitive type for take: ", *values.type);
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

const FunctionDoc take_doc(
    "Select values from an input based on indices from another array",
    ("The output is populated with values from the input at positions\n"
     "given by `indices`.  Nulls in `indices` emit null in the output."),
    {"input", "indices"}, "TakeOptions");

// Metafunction for dispatching to different Take implementations other than
// Array-Array.
class TakeMetaFunction : public MetaFunction {
 public:
  TakeMetaFunction()
      : MetaFunction("take", Arity::Binary(), take_doc, GetDefaultTakeOptions()) {}

  static Result<Datum> CallArrayTake(const std::vector<Datum>& args,
                                     const TakeOptions& options, ExecContext* ctx) {
    ARROW_ASSIGN_OR_RAISE(auto array_take_func,
                          ctx->func_registry()->GetFunction("array_take"));
    return array_take_func->Execute(args, &options, ctx);
  }

  static Result<std::shared_ptr<Array>> ChunkedArrayAsArray(
      const std::shared_ptr<ChunkedArray>& values, MemoryPool* pool) {
    switch (values->num_chunks()) {
      case 0:
        return MakeArrayOfNull(values->type(), /*length=*/0, pool);
      case 1:
        return values->chunk(0);
      default:
        return Concatenate(values->chunks(), pool);
    }
  }

 private:
  static Result<std::shared_ptr<ArrayData>> TakeAAA(const std::vector<Datum>& args,
                                                    const TakeOptions& options,
                                                    ExecContext* ctx) {
    DCHECK_EQ(args[0].kind(), Datum::ARRAY);
    DCHECK_EQ(args[1].kind(), Datum::ARRAY);
    ARROW_ASSIGN_OR_RAISE(Datum result, CallArrayTake(args, options, ctx));
    return result.array();
  }

  static Result<std::shared_ptr<ArrayData>> TakeCAA(
      const std::shared_ptr<ChunkedArray>& values, const Array& indices,
      const TakeOptions& options, ExecContext* ctx) {
    ARROW_ASSIGN_OR_RAISE(auto values_array,
                          ChunkedArrayAsArray(values, ctx->memory_pool()));
    std::vector<Datum> args = {std::move(values_array), indices};
    return TakeAAA(args, options, ctx);
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeCAC(
      const std::shared_ptr<ChunkedArray>& values, const Array& indices,
      const TakeOptions& options, ExecContext* ctx) {
    ARROW_ASSIGN_OR_RAISE(auto new_chunk, TakeCAA(values, indices, options, ctx));
    return std::make_shared<ChunkedArray>(MakeArray(std::move(new_chunk)));
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeCCC(
      const std::shared_ptr<ChunkedArray>& values,
      const std::shared_ptr<ChunkedArray>& indices, const TakeOptions& options,
      ExecContext* ctx) {
    // XXX: for every chunk in indices, values are gathered from all chunks in values to
    // form a new chunk in the result. Performing this concatenation is not ideal, but
    // greatly simplifies the implementation before something more efficient is
    // implemented.
    ARROW_ASSIGN_OR_RAISE(auto values_array,
                          ChunkedArrayAsArray(values, ctx->memory_pool()));
    std::vector<Datum> args = {std::move(values_array), {}};
    std::vector<std::shared_ptr<Array>> new_chunks;
    new_chunks.resize(indices->num_chunks());
    for (int i = 0; i < indices->num_chunks(); i++) {
      args[1] = indices->chunk(i);
      // XXX: this loop can use TakeCAA once it can handle ChunkedArray
      // without concatenating first
      ARROW_ASSIGN_OR_RAISE(auto chunk, TakeAAA(args, options, ctx));
      new_chunks[i] = MakeArray(chunk);
    }
    return std::make_shared<ChunkedArray>(std::move(new_chunks), values->type());
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeACC(const Array& values,
                                                       const ChunkedArray& indices,
                                                       const TakeOptions& options,
                                                       ExecContext* ctx) {
    auto num_chunks = indices.num_chunks();
    std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
    std::vector<Datum> args = {values, {}};
    for (int i = 0; i < num_chunks; i++) {
      // Take with that indices chunk
      args[1] = indices.chunk(i);
      ARROW_ASSIGN_OR_RAISE(auto chunk, TakeAAA(args, options, ctx));
      new_chunks[i] = MakeArray(chunk);
    }
    return std::make_shared<ChunkedArray>(std::move(new_chunks), values.type());
  }

  static Result<std::shared_ptr<RecordBatch>> TakeRAR(const RecordBatch& batch,
                                                      const Array& indices,
                                                      const TakeOptions& options,
                                                      ExecContext* ctx) {
    auto ncols = batch.num_columns();
    auto nrows = indices.length();
    std::vector<std::shared_ptr<Array>> columns(ncols);
    std::vector<Datum> args = {{}, indices};
    for (int j = 0; j < ncols; j++) {
      args[0] = batch.column(j);
      ARROW_ASSIGN_OR_RAISE(auto col_data, TakeAAA(args, options, ctx));
      columns[j] = MakeArray(col_data);
    }
    return RecordBatch::Make(batch.schema(), nrows, std::move(columns));
  }

  static Result<std::shared_ptr<Table>> TakeTAT(const std::shared_ptr<Table>& table,
                                                const Array& indices,
                                                const TakeOptions& options,
                                                ExecContext* ctx) {
    auto ncols = table->num_columns();
    std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

    for (int j = 0; j < ncols; j++) {
      ARROW_ASSIGN_OR_RAISE(columns[j], TakeCAC(table->column(j), indices, options, ctx));
    }
    return Table::Make(table->schema(), std::move(columns));
  }

  static Result<std::shared_ptr<Table>> TakeTCT(
      const std::shared_ptr<Table>& table, const std::shared_ptr<ChunkedArray>& indices,
      const TakeOptions& options, ExecContext* ctx) {
    auto ncols = table->num_columns();
    std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
    for (int j = 0; j < ncols; j++) {
      ARROW_ASSIGN_OR_RAISE(columns[j], TakeCCC(table->column(j), indices, options, ctx));
    }
    return Table::Make(table->schema(), std::move(columns));
  }

 public:
  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    Datum::Kind index_kind = args[1].kind();
    const auto& take_opts = static_cast<const TakeOptions&>(*options);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeAAA(args, take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeACC(*args[0].make_array(), *args[1].chunked_array(), take_opts, ctx);
        }
        break;
      case Datum::CHUNKED_ARRAY:
        if (index_kind == Datum::ARRAY) {
          return TakeCAC(args[0].chunked_array(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeCCC(args[0].chunked_array(), args[1].chunked_array(), take_opts,
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
          return TakeTAT(args[0].table(), *args[1].make_array(), take_opts, ctx);
        } else if (index_kind == Datum::CHUNKED_ARRAY) {
          return TakeTCT(args[0].table(), args[1].chunked_array(), take_opts, ctx);
        }
        break;
      case Datum::NONE:
      case Datum::SCALAR:
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
      {InputType(match::Primitive()), take_indices, FixedWidthTakeExec},
      {InputType(match::BinaryLike()), take_indices, VarBinaryTakeExec},
      {InputType(match::LargeBinaryLike()), take_indices, LargeVarBinaryTakeExec},
      {InputType(match::FixedSizeBinaryLike()), take_indices, FixedWidthTakeExec},
      {InputType(null()), take_indices, NullTakeExec},
      {InputType(Type::DICTIONARY), take_indices, DictionaryTake},
      {InputType(Type::EXTENSION), take_indices, ExtensionTake},
      {InputType(Type::LIST), take_indices, ListTakeExec},
      {InputType(Type::LARGE_LIST), take_indices, LargeListTakeExec},
      {InputType(Type::LIST_VIEW), take_indices, ListViewTakeExec},
      {InputType(Type::LARGE_LIST_VIEW), take_indices, LargeListViewTakeExec},
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
