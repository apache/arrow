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
#include <limits>
#include <type_traits>

#include "arrow/array/array_base.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/array/concatenate.h"
#include "arrow/array/data.h"
#include "arrow/buffer_builder.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bitmap_reader.h"
#include "arrow/util/int_util.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitmapReader;
using internal::GetArrayView;
using internal::IndexBoundsCheck;
using internal::OptionalBitBlockCounter;
using internal::OptionalBitIndexer;

namespace compute {
namespace internal {

using TakeState = OptionsWrapper<TakeOptions>;

// ----------------------------------------------------------------------
// Implement optimized take for primitive types from boolean to 1/2/4/8-byte
// C-type based types. Use common implementation for every byte width and only
// generate code for unsigned integer indices, since after boundschecking to
// check for negative numbers in the indices we can safely reinterpret_cast
// signed integers as unsigned.

struct PrimitiveTakeArgs {
  const uint8_t* values;
  const uint8_t* values_bitmap = nullptr;
  int values_bit_width;
  int64_t values_length;
  int64_t values_offset;
  int64_t values_null_count;
  const uint8_t* indices;
  const uint8_t* indices_bitmap = nullptr;
  int indices_bit_width;
  int64_t indices_length;
  int64_t indices_offset;
  int64_t indices_null_count;
};

// Reduce code size by dealing with the unboxing of the kernel inputs once
// rather than duplicating compiled code to do all these in each kernel.
PrimitiveTakeArgs GetPrimitiveTakeArgs(const ExecBatch& batch) {
  PrimitiveTakeArgs args;

  const ArrayData& arg0 = *batch[0].array();
  const ArrayData& arg1 = *batch[1].array();

  // Values
  args.values_bit_width = checked_cast<const FixedWidthType&>(*arg0.type).bit_width();
  args.values = arg0.buffers[1]->data();
  if (args.values_bit_width > 1) {
    args.values += arg0.offset * args.values_bit_width / 8;
  }
  args.values_length = arg0.length;
  args.values_offset = arg0.offset;
  args.values_null_count = arg0.GetNullCount();
  if (arg0.buffers[0]) {
    args.values_bitmap = arg0.buffers[0]->data();
  }

  // Indices
  args.indices_bit_width = checked_cast<const FixedWidthType&>(*arg1.type).bit_width();
  args.indices = arg1.buffers[1]->data() + arg1.offset * args.indices_bit_width / 8;
  args.indices_length = arg1.length;
  args.indices_offset = arg1.offset;
  args.indices_null_count = arg1.GetNullCount();
  if (arg1.buffers[0]) {
    args.indices_bitmap = arg1.buffers[0]->data();
  }

  return args;
}

/// \brief The Take implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather the physical C type. This way we
/// only generate one take function for each byte width.
///
/// This function assumes that the indices have been boundschecked.
template <typename IndexCType, typename ValueCType>
struct PrimitiveTakeImpl {
  static void Exec(const PrimitiveTakeArgs& args, Datum* out_datum) {
    auto values = reinterpret_cast<const ValueCType*>(args.values);
    auto values_bitmap = args.values_bitmap;
    auto values_offset = args.values_offset;

    auto indices = reinterpret_cast<const IndexCType*>(args.indices);
    auto indices_bitmap = args.indices_bitmap;
    auto indices_offset = args.indices_offset;

    ArrayData* out_arr = out_datum->mutable_array();
    auto out = out_arr->GetMutableValues<ValueCType>(1);
    auto out_bitmap = out_arr->buffers[0]->mutable_data();
    auto out_offset = out_arr->offset;

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (args.values_null_count > 0 || args.indices_null_count > 0) {
      BitUtil::SetBitsTo(out_bitmap, out_offset, args.indices_length, false);
    }

    OptionalBitBlockCounter indices_bit_counter(indices_bitmap, indices_offset,
                                                args.indices_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < args.indices_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (args.values_null_count == 0) {
        // Values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither values nor index nulls
          BitUtil::SetBitsTo(out_bitmap, out_offset + position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            out[position] = values[indices[position]];
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some indices but not all are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_bitmap, indices_offset + position)) {
              // index is not null
              BitUtil::SetBit(out_bitmap, out_offset + position);
              out[position] = values[indices[position]];
            } else {
              out[position] = ValueCType{};
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
            if (BitUtil::GetBit(values_bitmap, values_offset + indices[position])) {
              // value is not null
              out[position] = values[indices[position]];
              BitUtil::SetBit(out_bitmap, out_offset + position);
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
            if (BitUtil::GetBit(indices_bitmap, indices_offset + position) &&
                BitUtil::GetBit(values_bitmap, values_offset + indices[position])) {
              // index is not null && value is not null
              out[position] = values[indices[position]];
              BitUtil::SetBit(out_bitmap, out_offset + position);
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
  static void Exec(const PrimitiveTakeArgs& args, Datum* out_datum) {
    auto values = args.values;
    auto values_bitmap = args.values_bitmap;
    auto values_offset = args.values_offset;

    auto indices = reinterpret_cast<const IndexCType*>(args.indices);
    auto indices_bitmap = args.indices_bitmap;
    auto indices_offset = args.indices_offset;

    ArrayData* out_arr = out_datum->mutable_array();
    auto out = out_arr->buffers[1]->mutable_data();
    auto out_bitmap = out_arr->buffers[0]->mutable_data();
    auto out_offset = out_arr->offset;

    // If either the values or indices have nulls, we preemptively zero out the
    // out validity bitmap so that we don't have to use ClearBit in each
    // iteration for nulls.
    if (args.values_null_count > 0 || args.indices_null_count > 0) {
      BitUtil::SetBitsTo(out_bitmap, out_offset, args.indices_length, false);
    }
    // Avoid uninitialized data in values array
    BitUtil::SetBitsTo(out, out_offset, args.indices_length, false);

    auto PlaceDataBit = [&](int64_t loc, IndexCType index) {
      BitUtil::SetBitTo(out, out_offset + loc,
                        BitUtil::GetBit(values, values_offset + index));
    };

    OptionalBitBlockCounter indices_bit_counter(indices_bitmap, indices_offset,
                                                args.indices_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (position < args.indices_length) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (args.values_null_count == 0) {
        // Values are never null, so things are easier
        valid_count += block.popcount;
        if (block.popcount == block.length) {
          // Fastest path: neither values nor index nulls
          BitUtil::SetBitsTo(out_bitmap, out_offset + position, block.length, true);
          for (int64_t i = 0; i < block.length; ++i) {
            PlaceDataBit(position, indices[position]);
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_bitmap, indices_offset + position)) {
              // index is not null
              BitUtil::SetBit(out_bitmap, out_offset + position);
              PlaceDataBit(position, indices[position]);
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
            if (BitUtil::GetBit(values_bitmap, values_offset + indices[position])) {
              // value is not null
              BitUtil::SetBit(out_bitmap, out_offset + position);
              PlaceDataBit(position, indices[position]);
              ++valid_count;
            }
            ++position;
          }
        } else if (block.popcount > 0) {
          // Slow path: some but not all indices are null. Since we are doing
          // random access in general we have to check the value nullness one by
          // one.
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(indices_bitmap, indices_offset + position)) {
              // index is not null
              if (BitUtil::GetBit(values_bitmap, values_offset + indices[position])) {
                // value is not null
                PlaceDataBit(position, indices[position]);
                BitUtil::SetBit(out_bitmap, out_offset + position);
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
void TakeIndexDispatch(const PrimitiveTakeArgs& args, Datum* out) {
  // With the simplifying assumption that boundschecking has taken place
  // already at a higher level, we can now assume that the index values are all
  // non-negative. Thus, we can interpret signed integers as unsigned and avoid
  // having to generate double the amount of binary code to handle each integer
  // width.
  switch (args.indices_bit_width) {
    case 8:
      return TakeImpl<uint8_t, Args...>::Exec(args, out);
    case 16:
      return TakeImpl<uint16_t, Args...>::Exec(args, out);
    case 32:
      return TakeImpl<uint32_t, Args...>::Exec(args, out);
    case 64:
      return TakeImpl<uint64_t, Args...>::Exec(args, out);
    default:
      DCHECK(false) << "Invalid indices byte width";
      break;
  }
}

Status PreallocateData(KernelContext* ctx, int64_t length, int bit_width, Datum* out) {
  // Preallocate memory
  ArrayData* out_arr = out->mutable_array();
  out_arr->length = length;
  out_arr->buffers.resize(2);

  ARROW_ASSIGN_OR_RAISE(out_arr->buffers[0], ctx->AllocateBitmap(length));
  if (bit_width == 1) {
    ARROW_ASSIGN_OR_RAISE(out_arr->buffers[1], ctx->AllocateBitmap(length));
  } else {
    ARROW_ASSIGN_OR_RAISE(out_arr->buffers[1], ctx->Allocate(length * bit_width / 8));
  }
  return Status::OK();
}

static void PrimitiveTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  if (state.options.boundscheck) {
    KERNEL_RETURN_IF_ERROR(ctx, IndexBoundsCheck(*batch[1].array(), batch[0].length()));
  }
  PrimitiveTakeArgs args = GetPrimitiveTakeArgs(batch);
  KERNEL_RETURN_IF_ERROR(
      ctx, PreallocateData(ctx, args.indices_length, args.values_bit_width, out));
  switch (args.values_bit_width) {
    case 1:
      return TakeIndexDispatch<BooleanTakeImpl>(args, out);
    case 8:
      return TakeIndexDispatch<PrimitiveTakeImpl, int8_t>(args, out);
    case 16:
      return TakeIndexDispatch<PrimitiveTakeImpl, int16_t>(args, out);
    case 32:
      return TakeIndexDispatch<PrimitiveTakeImpl, int32_t>(args, out);
    case 64:
      return TakeIndexDispatch<PrimitiveTakeImpl, int64_t>(args, out);
    default:
      DCHECK(false) << "Invalid values byte width";
      break;
  }
}

static void NullTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  if (state.options.boundscheck) {
    KERNEL_RETURN_IF_ERROR(ctx, IndexBoundsCheck(*batch[1].array(), batch[0].length()));
  }
  out->value = std::make_shared<NullArray>(batch.length)->data();
}

static void DictionaryTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  DictionaryArray values(batch[0].array());
  Result<Datum> result =
      Take(Datum(values.indices()), batch[1], state.options, ctx->exec_context());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }
  DictionaryArray taken_values(values.type(), (*result).make_array(),
                               values.dictionary());
  out->value = taken_values.data();
}

static void ExtensionTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  ExtensionArray values(batch[0].array());
  Result<Datum> result =
      Take(Datum(values.storage()), batch[1], state.options, ctx->exec_context());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }

  ExtensionArray taken_values(values.type(), (*result).make_array());
  out->value = taken_values.data();
}

// ----------------------------------------------------------------------

// Use CRTP to dispatch to type-specific processing of indices for each
// unsigned integer type.
template <typename Impl, typename Type>
struct GenericTakeImpl {
  using ValuesArrayType = typename TypeTraits<Type>::ArrayType;

  KernelContext* ctx;
  std::shared_ptr<ArrayData> values;
  std::shared_ptr<ArrayData> indices;
  ArrayData* out;
  TypedBufferBuilder<bool> validity_builder;

  GenericTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : ctx(ctx),
        values(batch[0].array()),
        indices(batch[1].array()),
        out(out->mutable_array()),
        validity_builder(ctx->memory_pool()) {}

  virtual ~GenericTakeImpl() = default;

  Status FinishCommon() {
    out->buffers.resize(values->buffers.size());
    out->length = validity_builder.length();
    out->null_count = validity_builder.false_count();
    return validity_builder.Finish(&out->buffers[0]);
  }

  template <typename IndexCType, typename ValidVisitor, typename NullVisitor>
  Status VisitIndices(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
    const auto indices_values = indices->GetValues<IndexCType>(1);
    const uint8_t* bitmap = nullptr;
    if (indices->buffers[0]) {
      bitmap = indices->buffers[0]->data();
    }
    OptionalBitIndexer indices_is_valid(indices->buffers[0], indices->offset);
    OptionalBitIndexer values_is_valid(values->buffers[0], values->offset);
    const bool values_have_nulls = (values->GetNullCount() > 0);

    OptionalBitBlockCounter bit_counter(bitmap, indices->offset, indices->length);
    int64_t position = 0;
    while (position < indices->length) {
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

  virtual Status Init() { return Status::OK(); }

  // Implementation specific finish logic
  virtual Status Finish() = 0;

  Status Exec() {
    RETURN_NOT_OK(this->validity_builder.Reserve(indices->length));
    RETURN_NOT_OK(Init());
    int index_width =
        checked_cast<const FixedWidthType&>(*this->indices->type).bit_width() / 8;

    // CTRP dispatch here
    switch (index_width) {
      case 1:
        RETURN_NOT_OK(static_cast<Impl*>(this)->template ProcessIndices<uint8_t>());
        break;
      case 2:
        RETURN_NOT_OK(static_cast<Impl*>(this)->template ProcessIndices<uint16_t>());
        break;
      case 4:
        RETURN_NOT_OK(static_cast<Impl*>(this)->template ProcessIndices<uint32_t>());
        break;
      case 8:
        RETURN_NOT_OK(static_cast<Impl*>(this)->template ProcessIndices<uint64_t>());
        break;
      default:
        DCHECK(false) << "Invalid index width";
        break;
    }
    RETURN_NOT_OK(this->FinishCommon());
    return Finish();
  }
};

#define LIFT_BASE_MEMBERS()                               \
  using ValuesArrayType = typename Base::ValuesArrayType; \
  using Base::ctx;                                        \
  using Base::values;                                     \
  using Base::indices;                                    \
  using Base::out;                                        \
  using Base::validity_builder

static inline Status VisitNoop() { return Status::OK(); }

// A take implementation for 32-bit and 64-bit variable binary types. Common
// generated kernels are shared between Binary/String and
// LargeBinary/LargeString
template <typename Type>
struct VarBinaryTakeImpl : public GenericTakeImpl<VarBinaryTakeImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = GenericTakeImpl<VarBinaryTakeImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  std::shared_ptr<ArrayData> values_as_binary;
  TypedBufferBuilder<offset_type> offset_builder;
  TypedBufferBuilder<uint8_t> data_builder;

  static constexpr int64_t kOffsetLimit = std::numeric_limits<offset_type>::max() - 1;

  VarBinaryTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out),
        offset_builder(ctx->memory_pool()),
        data_builder(ctx->memory_pool()) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    ValuesArrayType typed_values(this->values_as_binary);

    // Presize the data builder with a rough estimate of the required data size
    const auto values_length = values->length;
    const auto mean_value_length =
        (values_length > 0) ? ((typed_values.raw_value_offsets()[values_length] -
                                typed_values.raw_value_offsets()[0]) /
                               static_cast<double>(values_length))
                            : 0.0;
    RETURN_NOT_OK(data_builder.Reserve(static_cast<int64_t>(
        mean_value_length * (indices->length - indices->GetNullCount()))));

    int64_t space_available = data_builder.capacity();

    offset_type offset = 0;
    RETURN_NOT_OK(this->template VisitIndices<IndexCType>(
        [&](IndexCType index) {
          offset_builder.UnsafeAppend(offset);
          auto val = typed_values.GetView(index);
          offset_type value_size = static_cast<offset_type>(val.size());
          if (ARROW_PREDICT_FALSE(static_cast<int64_t>(offset) +
                                  static_cast<int64_t>(value_size)) > kOffsetLimit) {
            return Status::Invalid("Take operation overflowed binary array capacity");
          }
          offset += value_size;
          if (ARROW_PREDICT_FALSE(value_size > space_available)) {
            RETURN_NOT_OK(data_builder.Reserve(value_size));
            space_available = data_builder.capacity() - data_builder.length();
          }
          data_builder.UnsafeAppend(reinterpret_cast<const uint8_t*>(val.data()),
                                    value_size);
          space_available -= value_size;
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
    return offset_builder.Reserve(indices->length + 1);
  }

  Status Finish() override {
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    return data_builder.Finish(&out->buffers[2]);
  }
};

struct FSBTakeImpl : public GenericTakeImpl<FSBTakeImpl, FixedSizeBinaryType> {
  using Base = GenericTakeImpl<FSBTakeImpl, FixedSizeBinaryType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<uint8_t> data_builder;

  FSBTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out), data_builder(ctx->memory_pool()) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    FixedSizeBinaryArray typed_values(this->values);
    int32_t value_size = typed_values.byte_width();

    RETURN_NOT_OK(data_builder.Reserve(value_size * indices->length));
    RETURN_NOT_OK(this->template VisitIndices<IndexCType>(
        [&](IndexCType index) {
          auto val = typed_values.GetView(index);
          data_builder.UnsafeAppend(reinterpret_cast<const uint8_t*>(val.data()),
                                    value_size);
          return Status::OK();
        },
        [&]() {
          data_builder.UnsafeAppend(value_size, static_cast<uint8_t>(0x00));
          return Status::OK();
        }));
    return Status::OK();
  }

  Status Finish() override { return data_builder.Finish(&out->buffers[1]); }
};

template <typename Type>
struct ListTakeImpl : public GenericTakeImpl<ListTakeImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = GenericTakeImpl<ListTakeImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<offset_type> offset_builder;
  typename TypeTraits<Type>::OffsetBuilderType child_index_builder;

  ListTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out),
        offset_builder(ctx->memory_pool()),
        child_index_builder(ctx->memory_pool()) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    ValuesArrayType typed_values(this->values);

    // TODO presize child_index_builder with a similar heuristic as VarBinaryTakeImpl

    offset_type offset = 0;
    auto PushValidIndex = [&](IndexCType index) {
      offset_builder.UnsafeAppend(offset);
      offset_type value_offset = typed_values.value_offset(index);
      offset_type value_length = typed_values.value_length(index);
      offset += value_length;
      RETURN_NOT_OK(child_index_builder.Reserve(value_length));
      for (offset_type j = value_offset; j < value_offset + value_length; ++j) {
        child_index_builder.UnsafeAppend(j);
      }
      return Status::OK();
    };

    auto PushNullIndex = [&]() {
      offset_builder.UnsafeAppend(offset);
      return Status::OK();
    };

    RETURN_NOT_OK(this->template VisitIndices<IndexCType>(std::move(PushValidIndex),
                                                          std::move(PushNullIndex)));
    offset_builder.UnsafeAppend(offset);
    return Status::OK();
  }

  Status Init() override {
    RETURN_NOT_OK(offset_builder.Reserve(indices->length + 1));
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

struct FSLTakeImpl : public GenericTakeImpl<FSLTakeImpl, FixedSizeListType> {
  Int64Builder child_index_builder;

  using Base = GenericTakeImpl<FSLTakeImpl, FixedSizeListType>;
  LIFT_BASE_MEMBERS();

  FSLTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out), child_index_builder(ctx->memory_pool()) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    ValuesArrayType typed_values(this->values);
    int32_t list_size = typed_values.list_type()->list_size();

    /// We must take list_size elements even for null elements of
    /// indices.
    RETURN_NOT_OK(child_index_builder.Reserve(indices->length * list_size));
    return this->template VisitIndices<IndexCType>(
        [&](IndexCType index) {
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

struct StructTakeImpl : public GenericTakeImpl<StructTakeImpl, StructType> {
  using Base = GenericTakeImpl<StructTakeImpl, StructType>;
  LIFT_BASE_MEMBERS();

  using Base::Base;

  template <typename IndexCType>
  Status ProcessIndices() {
    StructArray typed_values(values);
    return this->template VisitIndices<IndexCType>(
        [&](IndexCType index) { return Status::OK(); },
        /*visit_null=*/VisitNoop);
  }

  Status Finish() override {
    StructArray typed_values(values);

    // Select from children without boundschecking
    out->child_data.resize(values->type->num_fields());
    for (int field_index = 0; field_index < values->type->num_fields(); ++field_index) {
      ARROW_ASSIGN_OR_RAISE(Datum taken_field,
                            Take(Datum(typed_values.field(field_index)), Datum(indices),
                                 TakeOptions::NoBoundsCheck(), ctx->exec_context()));
      out->child_data[field_index] = taken_field.array();
    }
    return Status::OK();
  }
};

#undef LIFT_BASE_MEMBERS

template <typename Impl>
static void GenericTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  if (state.options.boundscheck) {
    KERNEL_RETURN_IF_ERROR(ctx, IndexBoundsCheck(*batch[1].array(), batch[0].length()));
  }
  Impl kernel(ctx, batch, out);
  KERNEL_RETURN_IF_ERROR(ctx, kernel.Exec());
}

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
    RETURN_NOT_OK(Concatenate(values.chunks(), default_memory_pool(), &current_chunk));
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
    RETURN_NOT_OK(
        Concatenate(current_chunk->chunks(), default_memory_pool(), &new_chunks[i]));
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
  return RecordBatch::Make(batch.schema(), nrows, columns);
}

Result<std::shared_ptr<Table>> TakeTA(const Table& table, const Array& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);

  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCA(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

Result<std::shared_ptr<Table>> TakeTC(const Table& table, const ChunkedArray& indices,
                                      const TakeOptions& options, ExecContext* ctx) {
  auto ncols = table.num_columns();
  std::vector<std::shared_ptr<ChunkedArray>> columns(ncols);
  for (int j = 0; j < ncols; j++) {
    ARROW_ASSIGN_OR_RAISE(columns[j], TakeCC(*table.column(j), indices, options, ctx));
  }
  return Table::Make(table.schema(), columns);
}

// Metafunction for dispatching to different Take implementations other than
// Array-Array.
//
// TODO: Revamp approach to executing Take operations. In addition to being
// overly complex dispatching, there is no parallelization.
class TakeMetaFunction : public MetaFunction {
 public:
  TakeMetaFunction() : MetaFunction("take", Arity::Binary()) {}

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

static InputType kTakeIndexType(match::Integer(), ValueDescr::ARRAY);

void RegisterVectorTake(FunctionRegistry* registry) {
  VectorKernel base;
  base.init = InitWrapOptions<TakeOptions>;
  base.can_execute_chunkwise = false;

  auto array_take = std::make_shared<VectorFunction>("array_take", Arity::Binary());

  auto AddTakeKernel = [&](InputType value_ty, ArrayKernelExec exec) {
    base.signature =
        KernelSignature::Make({value_ty, kTakeIndexType}, OutputType(FirstType));
    base.exec = exec;
    DCHECK_OK(array_take->AddKernel(base));
  };

  // Single kernel entry point for all primitive types. We dispatch to take
  // implementations inside the kernel for now. The primitive take
  // implementation writes into preallocated memory while the other
  // implementations handle their own memory allocation.
  AddTakeKernel(InputType(match::Primitive(), ValueDescr::ARRAY), PrimitiveTakeExec);

  // Take implementations for Binary, String, LargeBinary, LargeString, and
  // FixedSizeBinary
  AddTakeKernel(InputType(match::BinaryLike(), ValueDescr::ARRAY),
                GenericTakeExec<VarBinaryTakeImpl<BinaryType>>);
  AddTakeKernel(InputType(match::LargeBinaryLike(), ValueDescr::ARRAY),
                GenericTakeExec<VarBinaryTakeImpl<LargeBinaryType>>);
  AddTakeKernel(InputType::Array(Type::FIXED_SIZE_BINARY), GenericTakeExec<FSBTakeImpl>);

  AddTakeKernel(InputType::Array(null()), NullTakeExec);
  AddTakeKernel(InputType::Array(Type::DECIMAL), GenericTakeExec<FSBTakeImpl>);
  AddTakeKernel(InputType::Array(Type::DICTIONARY), DictionaryTakeExec);
  AddTakeKernel(InputType::Array(Type::EXTENSION), ExtensionTakeExec);
  AddTakeKernel(InputType::Array(Type::LIST), GenericTakeExec<ListTakeImpl<ListType>>);
  AddTakeKernel(InputType::Array(Type::LARGE_LIST),
                GenericTakeExec<ListTakeImpl<LargeListType>>);
  AddTakeKernel(InputType::Array(Type::FIXED_SIZE_LIST), GenericTakeExec<FSLTakeImpl>);
  AddTakeKernel(InputType::Array(Type::STRUCT), GenericTakeExec<StructTakeImpl>);

  // TODO: Reuse ListType kernel for MAP
  AddTakeKernel(InputType::Array(Type::MAP), GenericTakeExec<ListTakeImpl<MapType>>);

  DCHECK_OK(registry->AddFunction(std::move(array_take)));

  // Add take metafunction
  DCHECK_OK(registry->AddFunction(std::make_shared<TakeMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
