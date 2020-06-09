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

#include "arrow/array/array_base.h"
#include "arrow/array/array_primitive.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/record_batch.h"
#include "arrow/result.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/visitor_inline.h"

namespace arrow {

using internal::BitBlockCount;
using internal::BitBlockCounter;
using internal::BinaryBitBlockCounter;

namespace compute {
namespace internal {

using FilterState = OptionsWrapper<FilterOptions>;

int64_t FilterOutputSize(FilterOptions::NullSelectionBehavior null_selection,
                         const Array& arr) {
  const auto& filter = checked_cast<const BooleanArray&>(arr);
  const uint8_t* data_bitmap = arr.data()->buffers[1]->data();
  const int64_t offset = arr.offset();
  int64_t size = 0;
  if (filter.null_count() > 0) {
    const uint8_t* valid_bitmap = arr.null_bitmap_data();
    if (null_selection == FilterOptions::EMIT_NULL) {
      BitBlockCounter bit_counter(data_bitmap, offset, arr.length());
      int64_t position = 0;
      while (true) {
        BitBlockCount block = bit_counter.NextWord();
        if (block.popcount == block.length) {
          // The whole block is included
          size += block.length;
        } else {
          // If the filter is set or the filter is null, we include it in the
          // result
          for (int64_t i = 0; i < block.length; ++i) {
            if (BitUtil::GetBit(data_bitmap, offset + position + i) ||
                !BitUtil::GetBit(valid_bitmap, offset + position + i)) {
              ++size;
            }
          }
        }
        position += block.length;
      }
    } else {
      // FilterOptions::DROP_NULL. Values must be valid and on/true, so we can
      // use the binary block counter.
      BinaryBitBlockCounter bit_counter(data_bitmap, offset, valid_bitmap, offset,
                                        arr.length());
      while (true) {
        BitBlockCount block = bit_counter.NextAndWord();
        if (block.length == 0) {
          break;
        }
        size += block.popcount;
      }
    }
  } else {
    // The filter has no nulls, so we plow through it as fast as possible.
    BitBlockCounter bit_counter(data_bitmap, offset, arr.length());
    while (true) {
      BitBlockCount block = bit_counter.NextFourWords();
      if (block.length == 0) {
        break;
      }
      size += block.popcount;
    }
  }
  return size;
}

static int GetBitWidth(const DataType& type) {
  return checked_cast<const FixedWidthType&>(type).bit_width();
}

// ----------------------------------------------------------------------

/// \brief The Filter implementation for primitive (fixed-width) types does not
/// use the logical Arrow type but rather then physical C type. This way we
/// only generate one take function for each byte width.
template <typename ValueCType>
struct PrimitiveImpl {

  static void Exec(const ExecBatch& batch, Datum* out_datum) {
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
    while (true) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (block.length == 0) {
        // All indices processed.
        break;
      }
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
            }
            ++position;
          }
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
                out[position] = values[indices[position]];
                BitUtil::SetBit(out_bitmap, out_offset + position);
                ++valid_count;
              }
            }
            ++position;
          }
        }
      }
    }
    out_arr->null_count = out_arr->length - valid_count;
  }
};

template <typename IndexCType>
struct BooleanImpl {
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

    auto PlaceDataBit = [&](int64_t loc, IndexCType index) {
      BitUtil::SetBitTo(out, out_offset + loc,
                        BitUtil::GetBit(values, values_offset + index));
    };

    OptionalBitBlockCounter indices_bit_counter(indices_bitmap, indices_offset,
                                                args.indices_length);
    int64_t position = 0;
    int64_t valid_count = 0;
    while (true) {
      BitBlockCount block = indices_bit_counter.NextBlock();
      if (block.length == 0) {
        // All indices processed.
        break;
      }
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
        }
      }
    }
    out_arr->null_count = out_arr->length - valid_count;
  }
};

Status PreallocateFilter(KernelContext* ctx, int64_t length, int bit_width, Datum* out) {
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

static void PrimitiveExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const FilterState&>(*ctx->state());
  KERNEL_RETURN_IF_ERROR(
      ctx, PreallocateFilter(ctx, output_length, args.values_bit_width, out));
  switch (args.values_bit_width) {
    case 1:
      return BooleanImpl(batch, out);
    case 8:
      return PrimitiveImpl<int8_t>(batch, out);
    case 16:
      return PrimitiveImpl<int16_t>(batch, out);
    case 32:
      return PrimitiveImpl<int32_t>(batch, out);
    case 64:
      return PrimitiveImpl<int64_t>(batch, out);
    default:
      DCHECK(false) << "Invalid values byte width";
      break;
  }
}

static void NullExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  out->value = std::make_shared<NullArray>(batch.length)->data();
}

static void DictionaryExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const FilterState&>(*ctx->state());
  DictionaryArray values(batch[0].array());
  Result<Datum> result =
      Filter(Datum(values.indices()), batch[1], state.options, ctx->exec_context());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }
  DictionaryArray filtered_values(values.type(), (*result).make_array(),
                               values.dictionary());
  out->value = filtered_values.data();
}

static void ExtensionExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const FilterState&>(*ctx->state());
  ExtensionArray values(batch[0].array());
  Result<Datum> result =
      Filter(Datum(values.storage()), batch[1], state.options, ctx->exec_context());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }
  ExtensionArray filtered_values(values.type(), (*result).make_array());
  out->value = filtered_values.data();
}

// ----------------------------------------------------------------------

template <typename Type>
struct GenericImpl {
  using ValuesArrayType = typename TypeTraits<Type>::ArrayType;

  KernelContext* ctx;
  std::shared_ptr<ArrayData> values;
  std::shared_ptr<ArrayData> filter;
  ArrayData* out;
  TypedBufferBuilder<bool> validity_builder;

  GenericImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : ctx(ctx),
        values(batch[0].array()),
        filter(batch[1].array()),
        out(out->mutable_array()),
        validity_builder(ctx->memory_pool()) {}

  Status FinishCommon() {
    out->buffers.resize(values->buffers.size());
    out->length = validity_builder.length();
    out->null_count = validity_builder.false_count();
    return validity_builder.Finish(&out->buffers[0]);
  }

  template <typename InVisitor, typename NullVisitor>
  Status VisitFilter(InVisitor&& visit_in, NullVisitor&& visit_null) {
    const auto indices_values = indices.GetValues<IndexCType>(1);
    const uint8_t* bitmap = nullptr;
    if (indices.buffers[0]) {
      bitmap = indices.buffers[0]->data();
    }
    OptionalBitIndexer values_is_valid(values->buffers[0], values->offset);

    OptionalBitBlockCounter bit_counter(bitmap, indices.offset, indices.length);
    int64_t position = 0;
    while (position < indices.length) {
      BitBlockCount block = bit_counter.NextBlock();
      if (block.popcount == block.length) {
        // Fast path, no null indices
        for (int64_t i = 0; i < block.length; ++i) {
          if (values_is_valid[indices_values[position]]) {
            validity_builder.UnsafeAppend(true);
            RETURN_NOT_OK(visit_valid(indices_values[position]));
          } else {
            validity_builder.UnsafeAppend(false);
            RETURN_NOT_OK(visit_null());
          }
          ++position;
        }
      } else {
        for (int64_t i = 0; i < block.length; ++i) {
          if (BitUtil::GetBit(bitmap, indices.offset + position) &&
              values_is_valid[indices_values[position]]) {
            validity_builder.UnsafeAppend(true);
            RETURN_NOT_OK(visit_valid(indices_values[position]));
          } else {
            validity_builder.UnsafeAppend(false);
            RETURN_NOT_OK(visit_null());
          }
          ++position;
        }
      }
    }
    return Status::OK();
  }

  virtual Status Init() { return Status::OK(); }

  // Implementation specific finish logic
  virtual Status Finish() = 0;

  virtual Status ProcessFilter() = 0;

  virtual Status Exec() {
    RETURN_NOT_OK(this->validity_builder.Reserve(indices->length));
    RETURN_NOT_OK(Init());
    RETURN_NOT_OK(ProcessFilter());
    RETURN_NOT_OK(FinishCommon());
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
struct VarBinaryImpl : public GenericImpl<Type> {
  using offset_type = typename Type::offset_type;

  using Base = GenericImpl<Type>;
  LIFT_BASE_MEMBERS();

  std::shared_ptr<ArrayData> values_as_binary;
  TypedBufferBuilder<offset_type> offset_builder;
  TypedBufferBuilder<uint8_t> data_builder;

  VarBinaryImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out),
        offset_builder(ctx->memory_pool()),
        data_builder(ctx->memory_pool()) {}

  Status ProcessFilter() override {
    ValuesArrayType typed_values(this->values_as_binary);

    // Allocate at least 32K at a time to avoid having to call Reserve for
    // every value for lots of small strings
    static constexpr int64_t kAllocateChunksize = 1 << 15;
    RETURN_NOT_OK(data_builder.Reserve(kAllocateChunksize));
    int64_t space_available = data_builder.capacity();

    offset_type offset = 0;
    RETURN_NOT_OK(this->template VisitFilter(
        [&](IndexCType index) {
          offset_builder.UnsafeAppend(offset);
          auto val = typed_values.GetView(index);
          offset_type value_size = static_cast<offset_type>(val.size());
          offset += value_size;
          if (ARROW_PREDICT_FALSE(value_size > space_available)) {
            RETURN_NOT_OK(data_builder.Reserve(value_size + kAllocateChunksize));
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

struct FSBImpl : public GenericImpl<FixedSizeBinaryType> {
  using Base = GenericImpl<FixedSizeBinaryType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<uint8_t> data_builder;

  FSBImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out), data_builder(ctx->memory_pool()) {}

  Status ProcessFilter() {
    FixedSizeBinaryArray typed_values(this->values);
    int32_t value_size = typed_values.byte_width();

    RETURN_NOT_OK(data_builder.Reserve(value_size * indices->length));
    RETURN_NOT_OK(this->template VisitFilter(
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
struct ListImpl : public GenericImpl<ListImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = GenericImpl<Type>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<offset_type> offset_builder;
  typename TypeTraits<Type>::OffsetBuilderType child_index_builder;

  ListImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out),
        offset_builder(ctx->memory_pool()),
        child_index_builder(ctx->memory_pool()) {}

  Status ProcessFilter() {
    ValuesArrayType typed_values(this->values);

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

    RETURN_NOT_OK(this->template VisitFilter(
        *indices, std::move(PushValidIndex), std::move(PushNullIndex)));
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
                          Filter(*typed_values.values(), *child_indices,
                                 TakeOptions::NoBoundscheck(), ctx->exec_context()));
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    out->child_data = {taken_child->data()};
    return Status::OK();
  }
};

struct FSLImpl : public GenericImpl<FixedSizeListType> {
  Int64Builder child_index_builder;

  using Base = GenericImpl<FixedSizeListType>;
  LIFT_BASE_MEMBERS();

  FSLImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out), child_index_builder(ctx->memory_pool()) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    ValuesArrayType typed_values(this->values);
    int32_t list_size = typed_values.list_type()->list_size();

    /// We must take list_size elements even for null elements of
    /// indices.
    RETURN_NOT_OK(child_index_builder.Reserve(indices->length * list_size));
    return this->template VisitFilter(
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
                               TakeOptions::NoBoundscheck(), ctx->exec_context()));
    out->child_data = {taken_child->data()};
    return Status::OK();
  }
};

struct StructImpl : public GenericImpl<StructType> {
  using Base = GenericTakeImpl<StructType>;
  LIFT_BASE_MEMBERS();

  StructTakeImpl(KernelContext* ctx, const ExecBatch& batch, Datum* out)
      : Base(ctx, batch, out) {}

  template <typename IndexCType>
  Status ProcessIndices() {
    StructArray typed_values(values);
    return this->template VisitFilter(
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
                                 TakeOptions::NoBoundscheck(), ctx->exec_context()));
      out->child_data[field_index] = taken_field.array();
    }
    return Status::OK();
  }
};

template <typename Impl>
static void GenericTakeExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const auto& state = checked_cast<const TakeState&>(*ctx->state());
  Impl kernel(ctx, batch, out);
  KERNEL_RETURN_IF_ERROR(ctx, kernel.Exec());
}

// ----------------------------------------------------------------------

Result<std::shared_ptr<RecordBatch>> FilterRecordBatch(const RecordBatch& batch,
                                                       const Datum& filter,
                                                       const FunctionOptions* options,
                                                       ExecContext* ctx) {
  if (!filter.is_array()) {
    return Status::Invalid("Cannot filter a RecordBatch with a filter of kind ",
                           filter.kind());
  }

  const auto& filter_opts = *static_cast<const FilterOptions*>(options);
  // TODO: Rewrite this to convert to selection vector and use Take
  std::vector<std::shared_ptr<Array>> columns(batch.num_columns());
  for (int i = 0; i < batch.num_columns(); ++i) {
    ARROW_ASSIGN_OR_RAISE(Datum out,
                          Filter(batch.column(i)->data(), filter, filter_opts, ctx));
    columns[i] = out.make_array();
  }

  int64_t out_length;
  if (columns.size() == 0) {
    out_length =
        FilterOutputSize(filter_opts.null_selection_behavior, *filter.make_array());
  } else {
    out_length = columns[0]->length();
  }
  return RecordBatch::Make(batch.schema(), out_length, columns);
}

Result<std::shared_ptr<Table>> FilterTable(const Table& table, const Datum& filter,
                                           const FunctionOptions* options,
                                           ExecContext* ctx) {
  auto new_columns = table.columns();
  for (auto& column : new_columns) {
    ARROW_ASSIGN_OR_RAISE(
        Datum out_column,
        Filter(column, filter, *static_cast<const FilterOptions*>(options), ctx));
    column = out_column.chunked_array();
  }
  return Table::Make(table.schema(), std::move(new_columns));
}

class FilterMetaFunction : public MetaFunction {
 public:
  FilterMetaFunction() : MetaFunction("filter", Arity::Binary()) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
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

void RegisterVectorFilter(FunctionRegistry* registry) {
  VectorKernel base;
  base.init = InitWrapOptions<FilterOptions>;

  auto array_filter = std::make_shared<VectorFunction>("array_filter", Arity::Binary());
  InputType filter_ty = InputType::Array(boolean());
  OutputType out_ty(FirstType);

  auto AddKernel = [&](InputType value_ty, ArrayKernelExec exec) {
    base.signature =
        KernelSignature::Make({value_ty, filter_ty}, OutputType(FirstType));
    base.exec = exec;
    DCHECK_OK(array_take->AddKernel(base));
  };

  // Single kernel entry point for all primitive types
  AddKernel(InputType(match::Primitive(), ValueDescr::ARRAY), PrimitiveExec);

  // Implementations for Binary, String, LargeBinary, LargeString, and
  // FixedSizeBinary
  AddKernel(InputType(match::BinaryLike(), ValueDescr::ARRAY),
                GenericExec<VarBinaryImpl<BinaryType>>);
  AddKernel(InputType(match::LargeBinaryLike(), ValueDescr::ARRAY),
                GenericExec<VarBinaryImpl<LargeBinaryType>>);
  AddKernel(InputType::Array(Type::FIXED_SIZE_BINARY), GenericExec<FSBImpl>);

  AddKernel(InputType::Array(null()), NullExec);
  AddKernel(InputType::Array(Type::DECIMAL), GenericExec<FSBImpl>);
  AddKernel(InputType::Array(Type::DICTIONARY), DictionaryExec);
  AddKernel(InputType::Array(Type::EXTENSION), ExtensionExec);
  AddKernel(InputType::Array(Type::LIST), GenericExec<ListImpl<ListType>>);
  AddKernel(InputType::Array(Type::LARGE_LIST),
                GenericExec<ListImpl<LargeListType>>);
  AddKernel(InputType::Array(Type::FIXED_SIZE_LIST), GenericExec<FSLImpl>);
  AddKernel(InputType::Array(Type::STRUCT), GenericExec<StructImpl>);

  // TODO: Reuse ListType kernel for MAP
  AddKernel(InputType::Array(Type::MAP), GenericExec<ListImpl<MapType>>);

  DCHECK_OK(registry->AddFunction(std::move(array_filter)));

  // Add filter metafunction
  DCHECK_OK(registry->AddFunction(std::make_shared<FilterMetaFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
