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
#include "arrow/util/logging_internal.h"
#include "arrow/util/ree_util.h"

namespace arrow {

using internal::BinaryBitBlockCounter;
using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::CheckIndexBounds;
using internal::ChunkResolver;
using internal::OptionalBitBlockCounter;
using internal::TypedChunkLocation;

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

class ValuesSpan {
 private:
  const std::shared_ptr<ChunkedArray> chunked_ = nullptr;
  const ArraySpan chunk0_;  // first chunk or the whole array

 public:
  explicit ValuesSpan(const std::shared_ptr<ChunkedArray> values)
      : chunked_(std::move(values)), chunk0_{*values->chunk(0)->data()} {
    DCHECK(chunked_);
    DCHECK_GT(chunked_->num_chunks(), 0);
  }

  explicit ValuesSpan(const ArraySpan& values) : chunk0_(values) {}

  bool is_chunked() const { return chunked_ != nullptr; }

  const ChunkedArray& chunked_array() const {
    DCHECK(is_chunked());
    return *chunked_;
  }

  const ArraySpan& chunk0() const { return chunk0_; }

  const ArraySpan& array() const {
    DCHECK(!is_chunked());
    return chunk0_;
  }

  const DataType* type() const { return chunk0_.type; }

  int64_t length() const { return is_chunked() ? chunked_->length() : array().length; }

  bool MayHaveNulls() const {
    return is_chunked() ? chunked_->null_count() != 0 : array().MayHaveNulls();
  }
};

struct ChunkedFixedWidthValuesSpan {
 private:
  // src_residual_bit_offsets_[i] is used to store the bit offset of the first byte (0-7)
  // in src_chunks_[i] iff kValueWidthInBits == 1.
  std::vector<int> src_residual_bit_offsets;
  // Pre-computed pointers to the start of the values in each chunk.
  std::vector<const uint8_t*> src_chunks;

 public:
  ARROW_NOINLINE
  explicit ChunkedFixedWidthValuesSpan(const ChunkedArray& values) {
    const bool chunk_values_are_bit_sized = values.type()->id() == Type::BOOL;
    DCHECK_EQ(chunk_values_are_bit_sized, util::FixedWidthInBytes(*values.type()) == -1);
    if (chunk_values_are_bit_sized) {
      src_residual_bit_offsets.resize(values.num_chunks());
    }
    src_chunks.resize(values.num_chunks());

    for (int i = 0; i < values.num_chunks(); ++i) {
      const ArraySpan chunk{*values.chunk(i)->data()};
      DCHECK(util::IsFixedWidthLike(chunk));

      auto offset_pointer = util::OffsetPointerOfFixedBitWidthValues(chunk);
      if (chunk_values_are_bit_sized) {
        src_residual_bit_offsets[i] = offset_pointer.first;
      } else {
        DCHECK_EQ(offset_pointer.first, 0);
      }
      src_chunks[i] = offset_pointer.second;
    }
  }

  ARROW_NOINLINE
  ~ChunkedFixedWidthValuesSpan() = default;

  const int* src_residual_bit_offsets_data() const {
    return src_residual_bit_offsets.empty() ? nullptr : src_residual_bit_offsets.data();
  }

  const uint8_t* const* src_chunks_data() const { return src_chunks.data(); }
};

/// \brief Logical indices resolved against a chunked array.
struct ResolvedIndicesState {
 private:
  std::unique_ptr<Buffer> chunk_location_buffer = NULLPTR;

  ARROW_NOINLINE
  Status AllocateBuffers(int64_t n_indices, int64_t sizeof_index_type, MemoryPool* pool) {
    ARROW_ASSIGN_OR_RAISE(chunk_location_buffer,
                          AllocateBuffer(2 * n_indices * sizeof_index_type, pool));
    return Status::OK();
  }

 public:
  ARROW_NOINLINE
  ~ResolvedIndicesState() = default;

  template <typename IndexCType>
  Status InitWithIndices(const ArrayVector& chunks, int64_t idx_length,
                         const IndexCType* idx, MemoryPool* pool) {
    RETURN_NOT_OK(AllocateBuffers(idx_length, sizeof(IndexCType), pool));
    auto* chunk_location_vec =
        chunk_location_buffer->mutable_data_as<TypedChunkLocation<IndexCType>>();
    // All indices are resolved in one go without checking the validity bitmap.
    // This is OK as long the output corresponding to the invalid indices is not used.
    ChunkResolver resolver(chunks);
    bool enough_precision = resolver.ResolveMany<IndexCType>(
        /*n_indices=*/idx_length, /*logical_index_vec=*/idx, chunk_location_vec,
        /*chunk_hint=*/static_cast<IndexCType>(0));
    if (ARROW_PREDICT_FALSE(!enough_precision)) {
      return Status::IndexError("IndexCType is too small");
    }
    return Status::OK();
  }

  template <typename IndexCType>
  const TypedChunkLocation<IndexCType>* chunk_location_vec() const {
    return chunk_location_buffer->data_as<TypedChunkLocation<IndexCType>>();
  }
};

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

  static Status Exec(KernelContext* ctx, const ValuesSpan& values,
                     const ArraySpan& indices, ArrayData* out_arr, int64_t factor) {
#ifndef NDEBUG
    int64_t bit_width = util::FixedWidthInBits(*values.type());
    DCHECK(WithFactor::value || (kValueWidthInBits == bit_width && factor == 1));
    DCHECK(!WithFactor::value ||
           (factor > 0 && kValueWidthInBits == 8 &&  // factors are used with bytes
            static_cast<int64_t>(factor * kValueWidthInBits) == bit_width));
#endif
    return values.is_chunked()
               ? ChunkedExec(ctx, values.chunked_array(), indices, out_arr, factor)
               : Exec(ctx, values.array(), indices, out_arr, factor);
  }

  static Status Exec(KernelContext* ctx, const ArraySpan& values,
                     const ArraySpan& indices, ArrayData* out_arr, int64_t factor) {
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

  static Status ChunkedExec(KernelContext* ctx, const ChunkedArray& values,
                            const ArraySpan& indices, ArrayData* out_arr,
                            int64_t factor) {
    const bool out_has_validity = values.null_count() > 0 || indices.MayHaveNulls();

    ChunkedFixedWidthValuesSpan chunked_values{values};
    ResolvedIndicesState resolved_idx;
    RETURN_NOT_OK(resolved_idx.InitWithIndices<IndexCType>(
        /*chunks=*/values.chunks(), /*idx_length=*/indices.length,
        /*idx=*/indices.GetValues<IndexCType>(1), ctx->memory_pool()));

    int64_t valid_count = 0;
    arrow::internal::GatherFromChunks<kValueWidthInBits, IndexCType, WithFactor::value>
        gather{chunked_values.src_residual_bit_offsets_data(),
               chunked_values.src_chunks_data(),
               indices.length,
               resolved_idx.chunk_location_vec<IndexCType>(),
               /*out=*/util::MutableFixedWidthValuesPointer(out_arr),
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
Status TakeIndexDispatch(KernelContext* ctx, const ValuesSpan& values,
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

Status FixedWidthTakeExecImpl(KernelContext* ctx, const ValuesSpan& values,
                              const ArraySpan& indices, ArrayData* out_arr) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(indices, values.length()));
  }

  DCHECK(util::IsFixedWidthLike(values.chunk0()));
  // When we know for sure that values nor indices contain nulls, we can skip
  // allocating the validity bitmap altogether and save time and space.
  const bool allocate_validity = values.MayHaveNulls() || indices.MayHaveNulls();
  RETURN_NOT_OK(util::internal::PreallocateFixedWidthArrayData(
      ctx, indices.length, /*source=*/values.chunk0(), allocate_validity, out_arr));
  switch (util::FixedWidthInBits(*values.type())) {
    case 0:
      DCHECK(values.type()->id() == Type::FIXED_SIZE_BINARY ||
             values.type()->id() == Type::FIXED_SIZE_LIST);
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
  if (ARROW_PREDICT_TRUE(values.type()->id() == Type::FIXED_SIZE_BINARY ||
                         values.type()->id() == Type::FIXED_SIZE_LIST)) {
    int64_t byte_width = util::FixedWidthInBytes(*values.type());
    // 0-length fixed-size binary or lists were handled above on `case 0`
    DCHECK_GT(byte_width, 0);
    return TakeIndexDispatch<FixedWidthTakeImpl,
                             /*ValueBitWidth=*/std::integral_constant<int, 8>,
                             /*OutputIsZeroInitialized=*/std::false_type,
                             /*WithFactor=*/std::true_type>(ctx, values, indices, out_arr,
                                                            /*factor=*/byte_width);
  }
  return Status::NotImplemented("Unsupported primitive type for take: ", *values.type());
}

}  // namespace

Status FixedWidthTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  ValuesSpan values{batch[0].array};
  auto* out_arr = out->array_data().get();
  return FixedWidthTakeExecImpl(ctx, values, batch[1].array, out_arr);
}

Status FixedWidthTakeChunkedExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ValuesSpan values{batch[0].chunked_array()};
  auto& indices = batch[1].array();
  auto* out_arr = out->mutable_array();
  return FixedWidthTakeExecImpl(ctx, values, *indices, out_arr);
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
// [Chunked]Array-[Chunked]Array.
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

 private:
  static Result<std::shared_ptr<ArrayData>> TakeAAA(const std::vector<Datum>& args,
                                                    const TakeOptions& options,
                                                    ExecContext* ctx) {
    DCHECK_EQ(args[0].kind(), Datum::ARRAY);
    DCHECK_EQ(args[1].kind(), Datum::ARRAY);
    ARROW_ASSIGN_OR_RAISE(Datum result, CallArrayTake(args, options, ctx));
    return result.array();
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeCAC(const std::vector<Datum>& args,
                                                       const TakeOptions& options,
                                                       ExecContext* ctx) {
    // "array_take" can handle CA->C cases directly
    // (via their VectorKernel::exec_chunked)
    DCHECK_EQ(args[0].kind(), Datum::CHUNKED_ARRAY);
    DCHECK_EQ(args[1].kind(), Datum::ARRAY);
    ARROW_ASSIGN_OR_RAISE(auto result, CallArrayTake(args, options, ctx));
    return result.chunked_array();
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeCCC(const std::vector<Datum>& args,
                                                       const TakeOptions& options,
                                                       ExecContext* ctx) {
    // "array_take" can handle CC->C cases directly
    // (via their VectorKernel::exec_chunked)
    DCHECK_EQ(args[0].kind(), Datum::CHUNKED_ARRAY);
    DCHECK_EQ(args[1].kind(), Datum::CHUNKED_ARRAY);
    ARROW_ASSIGN_OR_RAISE(auto result, CallArrayTake(args, options, ctx));
    return result.chunked_array();
  }

  static Result<std::shared_ptr<ChunkedArray>> TakeACC(const std::vector<Datum>& args,
                                                       const TakeOptions& options,
                                                       ExecContext* ctx) {
    // "array_take" can handle AC->C cases directly
    // (via their VectorKernel::exec_chunked)
    DCHECK_EQ(args[0].kind(), Datum::ARRAY);
    DCHECK_EQ(args[1].kind(), Datum::CHUNKED_ARRAY);
    ARROW_ASSIGN_OR_RAISE(auto result, CallArrayTake(args, options, ctx));
    return result.chunked_array();
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
    std::vector<Datum> args = {/*placeholder*/ {}, indices};
    for (int j = 0; j < ncols; j++) {
      args[0] = table->column(j);
      ARROW_ASSIGN_OR_RAISE(columns[j], TakeCAC(args, options, ctx));
    }
    return Table::Make(table->schema(), std::move(columns));
  }

  static Result<std::shared_ptr<Table>> TakeTCT(
      const std::shared_ptr<Table>& table, const std::shared_ptr<ChunkedArray>& indices,
      const TakeOptions& options, ExecContext* ctx) {
    auto ncols = table->num_columns();
    std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
    std::vector<Datum> args = {/*placeholder*/ {}, indices};
    for (int j = 0; j < ncols; j++) {
      args[0] = table->column(j);
      ARROW_ASSIGN_OR_RAISE(columns[j], TakeCCC(args, options, ctx));
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
        // "array_take" can handle AA->A and AC->C cases directly
        // (via their VectorKernel::exec and VectorKernel::exec_chunked)
        if (index_kind == Datum::ARRAY || index_kind == Datum::CHUNKED_ARRAY) {
          return CallArrayTake(args, take_opts, ctx);
        }
        break;
      case Datum::CHUNKED_ARRAY:
        // "array_take" can handle CA->C and CC->C cases directly
        // (via their VectorKernel::exec_chunked)
        if (index_kind == Datum::ARRAY || index_kind == Datum::CHUNKED_ARRAY) {
          return CallArrayTake(args, take_opts, ctx);
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
        args[0].ToString(), ", indices=", args[1].ToString());
  }
};

// ----------------------------------------------------------------------

/// \brief Prepare the output array like ExecuteArrayKernel::PrepareOutput()
std::shared_ptr<ArrayData> PrepareOutput(const ExecBatch& batch, int64_t length) {
  DCHECK_EQ(batch.length, length);
  auto out = std::make_shared<ArrayData>(batch.values[0].type(), length);
  out->buffers.resize(batch.values[0].type()->layout().buffers.size());
  return out;
}

Result<std::shared_ptr<Array>> ChunkedArrayAsArray(
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

Status CallAAAKernel(ArrayKernelExec take_aaa_exec, KernelContext* ctx,
                     std::shared_ptr<ArrayData> values,
                     std::shared_ptr<ArrayData> indices, Datum* out) {
  int64_t batch_length = values->length;
  std::vector<Datum> args = {std::move(values), std::move(indices)};
  ExecBatch array_array_batch(std::move(args), batch_length);
  DCHECK_EQ(out->kind(), Datum::ARRAY);
  ExecSpan exec_span{array_array_batch};
  ExecResult result;
  result.value = out->array();
  return take_aaa_exec(ctx, exec_span, &result);
}

Status CallCAAKernel(VectorKernel::ChunkedExec take_caa_exec, KernelContext* ctx,
                     std::shared_ptr<ChunkedArray> values,
                     std::shared_ptr<ArrayData> indices, Datum* out) {
  int64_t batch_length = values->length();
  std::vector<Datum> args = {std::move(values), std::move(indices)};
  ExecBatch chunked_array_array_batch(std::move(args), batch_length);
  DCHECK_EQ(out->kind(), Datum::ARRAY);
  return take_caa_exec(ctx, chunked_array_array_batch, out);
}

Status TakeACCChunkedExec(ArrayKernelExec take_aaa_exec, KernelContext* ctx,
                          const ExecBatch& batch, Datum* out) {
  auto& values = batch.values[0].array();
  auto& indices = batch.values[1].chunked_array();
  auto num_chunks = indices->num_chunks();
  std::vector<std::shared_ptr<Array>> new_chunks(num_chunks);
  for (int i = 0; i < num_chunks; i++) {
    // Take with that indices chunk
    auto& indices_chunk = indices->chunk(i)->data();
    Datum result = PrepareOutput(batch, values->length);
    RETURN_NOT_OK(CallAAAKernel(take_aaa_exec, ctx, values, indices_chunk, &result));
    new_chunks[i] = MakeArray(result.array());
  }
  out->value = std::make_shared<ChunkedArray>(std::move(new_chunks), values->type);
  return Status::OK();
}

/// \brief Generic (slower) VectorKernel::exec_chunked (`CA->C`, `CC->C`, and `AC->C`).
///
/// This function concatenates the chunks of the values and then calls the `AA->A` take
/// kernel to handle the `CA->C` cases. The ArrayData returned by the `AA->A` kernel is
/// converted to a ChunkedArray with a single chunk to honor the `CA->C` contract.
///
/// For `CC->C` cases, it concatenates the chunks of the values and calls the `AA->A` take
/// kernel for each chunk of the indices, producing a new chunked array with the same
/// shape as the indices.
///
/// `AC->C` cases are trivially delegated to TakeACCChunkedExec without any concatenation.
///
/// \param take_aaa_exec The `AA->A` take kernel to use.
Status GenericTakeChunkedExec(ArrayKernelExec take_aaa_exec, KernelContext* ctx,
                              const ExecBatch& batch, Datum* out) {
  const auto& args = batch.values;
  if (args[0].kind() == Datum::CHUNKED_ARRAY) {
    auto& values_chunked = args[0].chunked_array();
    ARROW_ASSIGN_OR_RAISE(auto values_array,
                          ChunkedArrayAsArray(values_chunked, ctx->memory_pool()));
    if (args[1].kind() == Datum::ARRAY) {
      // CA->C
      auto& indices = args[1].array();
      DCHECK_EQ(values_array->length(), batch.length);
      {
        // AA->A
        RETURN_NOT_OK(
            CallAAAKernel(take_aaa_exec, ctx, values_array->data(), indices, out));
        out->value = std::make_shared<ChunkedArray>(MakeArray(out->array()));
      }
      return Status::OK();
    } else if (args[1].kind() == Datum::CHUNKED_ARRAY) {
      // CC->C
      const auto& indices = args[1].chunked_array();
      std::vector<std::shared_ptr<Array>> new_chunks;
      for (int i = 0; i < indices->num_chunks(); i++) {
        // AA->A
        auto& indices_chunk = indices->chunk(i)->data();
        Datum result = PrepareOutput(batch, values_array->length());
        RETURN_NOT_OK(CallAAAKernel(take_aaa_exec, ctx, values_array->data(),
                                    indices_chunk, &result));
        new_chunks.push_back(MakeArray(result.array()));
      }
      DCHECK(out->is_array());
      out->value =
          std::make_shared<ChunkedArray>(std::move(new_chunks), values_chunked->type());
      return Status::OK();
    }
  } else {
    // VectorKernel::exec_chunked are only called when at least one of the inputs is
    // chunked, so we should be able to assume that args[1] is a chunked array when
    // everything is wired up correctly.
    if (args[1].kind() == Datum::CHUNKED_ARRAY) {
      // AC->C
      return TakeACCChunkedExec(take_aaa_exec, ctx, batch, out);
    } else {
      DCHECK(false) << "Unexpected kind for array_take's exec_chunked kernel: values="
                    << args[0].ToString() << ", indices=" << args[1].ToString();
    }
  }
  return Status::NotImplemented(
      "Unsupported kinds for 'array_take', try using 'take': "
      "values=",
      args[0].ToString(), ", indices=", args[1].ToString());
}

template <ArrayKernelExec kTakeAAAExec>
struct GenericTakeChunkedExecFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return GenericTakeChunkedExec(kTakeAAAExec, ctx, batch, out);
  }
};

/// \brief Specialized (faster) VectorKernel::exec_chunked (`CA->C`, `CC->C`, `AC->C`).
///
/// This function doesn't ever need to concatenate the chunks of the values, so it can be
/// more efficient than GenericTakeChunkedExec that can only delegate to the `AA->A` take
/// kernels.
///
/// For `CA->C` cases, it can call the `CA->A` take kernel directly [1] and trivially
/// convert the result to a ChunkedArray of a single chunk to honor the `CA->C` contract.
///
/// For `CC->C` cases it can call the `CA->A` take kernel for each chunk of the indices to
/// get each chunk that becomes the ChunkedArray output.
///
/// `AC->C` cases are trivially delegated to TakeACCChunkedExec.
///
/// \param take_aaa_exec The `AA->A` take kernel to use.
Status SpecialTakeChunkedExec(const ArrayKernelExec take_aaa_exec,
                              VectorKernel::ChunkedExec take_caa_exec, KernelContext* ctx,
                              const ExecBatch& batch, Datum* out) {
  Datum result = PrepareOutput(batch, batch.length);
  auto* pool = ctx->memory_pool();
  const auto& args = batch.values;
  if (args[0].kind() == Datum::CHUNKED_ARRAY) {
    auto& values_chunked = args[0].chunked_array();
    std::shared_ptr<Array> single_chunk = nullptr;
    if (values_chunked->num_chunks() == 0 || values_chunked->length() == 0) {
      ARROW_ASSIGN_OR_RAISE(single_chunk,
                            MakeArrayOfNull(values_chunked->type(), /*length=*/0, pool));
    } else if (values_chunked->num_chunks() == 1) {
      single_chunk = values_chunked->chunk(0);
    }

    if (args[1].kind() == Datum::ARRAY) {
      // CA->C
      auto& indices = args[1].array();
      if (single_chunk) {
        // AA->A
        DCHECK_EQ(single_chunk->length(), batch.length);
        // If the ChunkedArray was cheaply converted to a single chunk,
        // we can use the AA->A take kernel directly.
        RETURN_NOT_OK(
            CallAAAKernel(take_aaa_exec, ctx, single_chunk->data(), indices, out));
        out->value = std::make_shared<ChunkedArray>(MakeArray(out->array()));
        return Status::OK();
      }
      // Instead of concatenating the chunks, we call the CA->A take kernel
      // which has a more efficient implementation for this case. At this point,
      // that implementation doesn't have to care about empty or single-chunk
      // ChunkedArrays.
      RETURN_NOT_OK(take_caa_exec(ctx, batch, &result));
      out->value = std::make_shared<ChunkedArray>(MakeArray(result.array()));
      return Status::OK();
    } else {
      // CC->C
      const auto& indices = args[1].chunked_array();
      std::vector<std::shared_ptr<Array>> new_chunks;
      for (int i = 0; i < indices->num_chunks(); i++) {
        auto& indices_chunk = indices->chunk(i)->data();
        result = PrepareOutput(batch, values_chunked->length());
        if (single_chunk) {
          // If the ChunkedArray was cheaply converted to a single chunk,
          // we can use the AA->A take kernel directly.
          RETURN_NOT_OK(CallAAAKernel(take_aaa_exec, ctx, single_chunk->data(),
                                      indices_chunk, &result));
        } else {
          RETURN_NOT_OK(
              CallCAAKernel(take_caa_exec, ctx, values_chunked, indices_chunk, &result));
        }
        new_chunks.push_back(MakeArray(result.array()));
      }
      DCHECK(out->is_array());
      out->value =
          std::make_shared<ChunkedArray>(std::move(new_chunks), values_chunked->type());
      return Status::OK();
    }
  } else {
    // VectorKernel::exec_chunked are only called when at least one of the inputs is
    // chunked, so we should be able to assume that args[1] is a chunked array when
    // everything is wired up correctly.
    if (args[1].kind() == Datum::CHUNKED_ARRAY) {
      // AC->C
      return TakeACCChunkedExec(take_aaa_exec, ctx, batch, out);
    } else {
      DCHECK(false) << "Unexpected kind for array_take's exec_chunked kernel: values="
                    << args[0].ToString() << ", indices=" << args[1].ToString();
    }
  }
  return Status::NotImplemented(
      "Unsupported kinds for 'array_take', try using 'take': "
      "values=",
      args[0].ToString(), ", indices=", args[1].ToString());
}

template <ArrayKernelExec kTakeAAAExec, VectorKernel::ChunkedExec kTakeCAAExec>
struct SpecialTakeChunkedExecFunctor {
  static Status Exec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
    return SpecialTakeChunkedExec(kTakeAAAExec, kTakeCAAExec, ctx, batch, out);
  }
};

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
      {InputType(match::Primitive()), take_indices, FixedWidthTakeExec,
       SpecialTakeChunkedExecFunctor<FixedWidthTakeExec,
                                     FixedWidthTakeChunkedExec>::Exec},
      {InputType(match::BinaryLike()), take_indices, VarBinaryTakeExec,
       GenericTakeChunkedExecFunctor<VarBinaryTakeExec>::Exec},
      {InputType(match::LargeBinaryLike()), take_indices, LargeVarBinaryTakeExec,
       GenericTakeChunkedExecFunctor<LargeVarBinaryTakeExec>::Exec},
      {InputType(match::FixedSizeBinaryLike()), take_indices, FixedWidthTakeExec,
       SpecialTakeChunkedExecFunctor<FixedWidthTakeExec,
                                     FixedWidthTakeChunkedExec>::Exec},
      {InputType(null()), take_indices, NullTakeExec,
       GenericTakeChunkedExecFunctor<NullTakeExec>::Exec},
      {InputType(Type::DICTIONARY), take_indices, DictionaryTake,
       GenericTakeChunkedExecFunctor<DictionaryTake>::Exec},
      {InputType(Type::EXTENSION), take_indices, ExtensionTake,
       GenericTakeChunkedExecFunctor<ExtensionTake>::Exec},
      {InputType(Type::LIST), take_indices, ListTakeExec,
       GenericTakeChunkedExecFunctor<ListTakeExec>::Exec},
      {InputType(Type::LARGE_LIST), take_indices, LargeListTakeExec,
       GenericTakeChunkedExecFunctor<LargeListTakeExec>::Exec},
      {InputType(Type::LIST_VIEW), take_indices, ListViewTakeExec,
       GenericTakeChunkedExecFunctor<ListViewTakeExec>::Exec},
      {InputType(Type::LARGE_LIST_VIEW), take_indices, LargeListViewTakeExec,
       GenericTakeChunkedExecFunctor<LargeListViewTakeExec>::Exec},
      {InputType(Type::FIXED_SIZE_LIST), take_indices, FSLTakeExec,
       GenericTakeChunkedExecFunctor<FSLTakeExec>::Exec},
      {InputType(Type::DENSE_UNION), take_indices, DenseUnionTakeExec,
       GenericTakeChunkedExecFunctor<DenseUnionTakeExec>::Exec},
      {InputType(Type::SPARSE_UNION), take_indices, SparseUnionTakeExec,
       GenericTakeChunkedExecFunctor<SparseUnionTakeExec>::Exec},
      {InputType(Type::STRUCT), take_indices, StructTakeExec,
       GenericTakeChunkedExecFunctor<StructTakeExec>::Exec},
      {InputType(Type::MAP), take_indices, MapTakeExec,
       GenericTakeChunkedExecFunctor<MapTakeExec>::Exec},
  };
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
