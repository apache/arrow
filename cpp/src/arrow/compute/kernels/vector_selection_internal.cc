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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "arrow/array/array_binary.h"
#include "arrow/array/array_nested.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer_builder.h"
#include "arrow/chunked_array.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/type.h"
#include "arrow/type_traits.h"
#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/int_util.h"
#include "arrow/util/logging.h"

namespace arrow {

using internal::CheckIndexBounds;

namespace compute {
namespace internal {

void RegisterSelectionFunction(const std::string& name, FunctionDoc doc,
                               VectorKernel base_kernel, InputType selection_type,
                               const std::vector<SelectionKernelData>& kernels,
                               const FunctionOptions* default_options,
                               FunctionRegistry* registry) {
  auto func = std::make_shared<VectorFunction>(name, Arity::Binary(), std::move(doc),
                                               default_options);
  for (auto& kernel_data : kernels) {
    base_kernel.signature =
        KernelSignature::Make({std::move(kernel_data.input), selection_type}, FirstType);
    base_kernel.exec = kernel_data.exec;
    DCHECK_OK(func->AddKernel(base_kernel));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

Status PreallocatePrimitiveArrayData(KernelContext* ctx, int64_t length, int bit_width,
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

namespace {

using FilterState = OptionsWrapper<FilterOptions>;
using TakeState = OptionsWrapper<TakeOptions>;

// ----------------------------------------------------------------------
// Implement take for other data types where there is less performance
// sensitivity by visiting the selected indices.

// Use CRTP to dispatch to type-specific processing of take indices for each
// unsigned integer type.
template <typename Impl, typename Type>
struct Selection {
  using ValuesArrayType = typename TypeTraits<Type>::ArrayType;

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

  KernelContext* ctx;
  const ArraySpan& values;
  const ArraySpan& selection;
  int64_t output_length;
  ArrayData* out;
  TypedBufferBuilder<bool> validity_builder;

  Selection(KernelContext* ctx, const ExecSpan& batch, int64_t output_length,
            ExecResult* out)
      : ctx(ctx),
        values(batch[0].array),
        selection(batch[1].array),
        output_length(output_length),
        out(out->array_data().get()),
        validity_builder(ctx->memory_pool()) {}

  virtual ~Selection() = default;

  Status FinishCommon() {
    out->buffers.resize(values.num_buffers());
    out->length = validity_builder.length();
    out->null_count = validity_builder.false_count();
    return validity_builder.Finish(&out->buffers[0]);
  }

  template <typename IndexCType, typename ValidVisitor, typename NullVisitor>
  Status VisitTake(ValidVisitor&& visit_valid, NullVisitor&& visit_null) {
    const auto indices_values = selection.GetValues<IndexCType>(1);
    const uint8_t* is_valid = selection.buffers[0].data;
    arrow::internal::OptionalBitIndexer indices_is_valid(is_valid, selection.offset);
    arrow::internal::OptionalBitIndexer values_is_valid(values.buffers[0].data,
                                                        values.offset);

    const bool values_have_nulls = values.MayHaveNulls();
    arrow::internal::OptionalBitBlockCounter bit_counter(is_valid, selection.offset,
                                                         selection.length);
    int64_t position = 0;
    while (position < selection.length) {
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

    const uint8_t* filter_data = selection.buffers[1].data;

    const uint8_t* filter_is_valid = selection.buffers[0].data;
    const int64_t filter_offset = selection.offset;
    arrow::internal::OptionalBitIndexer values_is_valid(values.buffers[0].data,
                                                        values.offset);

    // We use 3 block counters for fast scanning of the filter
    //
    // * values_valid_counter: for values null/not-null
    // * filter_valid_counter: for filter null/not-null
    // * filter_counter: for filter true/false
    arrow::internal::OptionalBitBlockCounter values_valid_counter(
        values.buffers[0].data, values.offset, values.length);
    arrow::internal::OptionalBitBlockCounter filter_valid_counter(
        filter_is_valid, filter_offset, selection.length);
    arrow::internal::BitBlockCounter filter_counter(filter_data, filter_offset,
                                                    selection.length);
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

    while (in_position < selection.length) {
      arrow::internal::BitBlockCount filter_valid_block = filter_valid_counter.NextWord();
      arrow::internal::BitBlockCount values_valid_block = values_valid_counter.NextWord();
      arrow::internal::BitBlockCount filter_block = filter_counter.NextWord();
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
              if (bit_util::GetBit(filter_data, filter_offset + in_position)) {
                RETURN_NOT_OK(AppendNotNull(in_position));
              }
              ++in_position;
            }
          } else {
            // Some of the values in the block are null, so we have to check
            // each one
            for (int64_t i = 0; i < filter_block.length; ++i) {
              if (bit_util::GetBit(filter_data, filter_offset + in_position)) {
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
            if (bit_util::GetBit(filter_is_valid, filter_offset + in_position) &&
                bit_util::GetBit(filter_data, filter_offset + in_position)) {
              RETURN_NOT_OK(AppendMaybeNull(in_position));
            }
            ++in_position;
          }
        } else {
          // Filter null values are appended to output as null whether the
          // value in the corresponding slot is valid or not
          for (int64_t i = 0; i < filter_block.length; ++i) {
            const bool filter_not_null =
                bit_util::GetBit(filter_is_valid, filter_offset + in_position);
            if (filter_not_null &&
                bit_util::GetBit(filter_data, filter_offset + in_position)) {
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
    int index_width = this->selection.type->byte_width();

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

inline Status VisitNoop() { return Status::OK(); }

// A selection implementation for 32-bit and 64-bit variable binary
// types. Common generated kernels are shared between Binary/String and
// LargeBinary/LargeString
template <typename Type>
struct VarBinarySelectionImpl : public Selection<VarBinarySelectionImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = Selection<VarBinarySelectionImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<offset_type> offset_builder;
  TypedBufferBuilder<uint8_t> data_builder;

  static constexpr int64_t kOffsetLimit = std::numeric_limits<offset_type>::max() - 1;

  VarBinarySelectionImpl(KernelContext* ctx, const ExecSpan& batch, int64_t output_length,
                         ExecResult* out)
      : Base(ctx, batch, output_length, out),
        offset_builder(ctx->memory_pool()),
        data_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    const auto raw_offsets = this->values.template GetValues<offset_type>(1);
    const uint8_t* raw_data = this->values.buffers[2].data;

    // Presize the data builder with a rough estimate of the required data size
    if (this->values.length > 0) {
      int64_t data_length = raw_offsets[this->values.length] - raw_offsets[0];
      const double mean_value_length =
          data_length / static_cast<double>(this->values.length);

      // TODO: See if possible to reduce output_length for take/filter cases
      // where there are nulls in the selection array
      RETURN_NOT_OK(
          data_builder.Reserve(static_cast<int64_t>(mean_value_length * output_length)));
    }
    int64_t space_available = data_builder.capacity();

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

  Status Init() override { return offset_builder.Reserve(output_length + 1); }

  Status Finish() override {
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    return data_builder.Finish(&out->buffers[2]);
  }
};

struct FSBSelectionImpl : public Selection<FSBSelectionImpl, FixedSizeBinaryType> {
  using Base = Selection<FSBSelectionImpl, FixedSizeBinaryType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<uint8_t> data_builder;

  FSBSelectionImpl(KernelContext* ctx, const ExecSpan& batch, int64_t output_length,
                   ExecResult* out)
      : Base(ctx, batch, output_length, out), data_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    FixedSizeBinaryArray typed_values(this->values.ToArrayData());
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
struct ListSelectionImpl : public Selection<ListSelectionImpl<Type>, Type> {
  using offset_type = typename Type::offset_type;

  using Base = Selection<ListSelectionImpl<Type>, Type>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<offset_type> offset_builder;
  typename TypeTraits<Type>::OffsetBuilderType child_index_builder;

  ListSelectionImpl(KernelContext* ctx, const ExecSpan& batch, int64_t output_length,
                    ExecResult* out)
      : Base(ctx, batch, output_length, out),
        offset_builder(ctx->memory_pool()),
        child_index_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    ValuesArrayType typed_values(this->values.ToArrayData());

    // TODO presize child_index_builder with a similar heuristic as VarBinarySelectionImpl

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

    ValuesArrayType typed_values(this->values.ToArrayData());

    // No need to boundscheck the child values indices
    ARROW_ASSIGN_OR_RAISE(std::shared_ptr<Array> taken_child,
                          Take(*typed_values.values(), *child_indices,
                               TakeOptions::NoBoundsCheck(), ctx->exec_context()));
    RETURN_NOT_OK(offset_builder.Finish(&out->buffers[1]));
    out->child_data = {taken_child->data()};
    return Status::OK();
  }
};

struct DenseUnionSelectionImpl
    : public Selection<DenseUnionSelectionImpl, DenseUnionType> {
  using Base = Selection<DenseUnionSelectionImpl, DenseUnionType>;
  LIFT_BASE_MEMBERS();

  TypedBufferBuilder<int32_t> value_offset_buffer_builder_;
  TypedBufferBuilder<int8_t> child_id_buffer_builder_;
  std::vector<int8_t> type_codes_;
  std::vector<Int32Builder> child_indices_builders_;

  DenseUnionSelectionImpl(KernelContext* ctx, const ExecSpan& batch,
                          int64_t output_length, ExecResult* out)
      : Base(ctx, batch, output_length, out),
        value_offset_buffer_builder_(ctx->memory_pool()),
        child_id_buffer_builder_(ctx->memory_pool()),
        type_codes_(checked_cast<const UnionType&>(*this->values.type).type_codes()),
        child_indices_builders_(type_codes_.size()) {
    for (auto& child_indices_builder : child_indices_builders_) {
      child_indices_builder = Int32Builder(ctx->memory_pool());
    }
  }

  template <typename Adapter>
  Status GenerateOutput() {
    DenseUnionArray typed_values(this->values.ToArrayData());
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
    DenseUnionArray typed_values(this->values.ToArrayData());
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

struct FSLSelectionImpl : public Selection<FSLSelectionImpl, FixedSizeListType> {
  Int64Builder child_index_builder;

  using Base = Selection<FSLSelectionImpl, FixedSizeListType>;
  LIFT_BASE_MEMBERS();

  FSLSelectionImpl(KernelContext* ctx, const ExecSpan& batch, int64_t output_length,
                   ExecResult* out)
      : Base(ctx, batch, output_length, out), child_index_builder(ctx->memory_pool()) {}

  template <typename Adapter>
  Status GenerateOutput() {
    ValuesArrayType typed_values(this->values.ToArrayData());
    const int32_t list_size = typed_values.list_type()->list_size();
    const int64_t base_offset = typed_values.offset();

    // We must take list_size elements even for null elements of
    // indices.
    RETURN_NOT_OK(child_index_builder.Reserve(output_length * list_size));

    Adapter adapter(this);
    return adapter.Generate(
        [&](int64_t index) {
          int64_t offset = (base_offset + index) * list_size;
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

    ValuesArrayType typed_values(this->values.ToArrayData());

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
struct StructSelectionImpl : public Selection<StructSelectionImpl, StructType> {
  using Base = Selection<StructSelectionImpl, StructType>;
  LIFT_BASE_MEMBERS();
  using Base::Base;

  template <typename Adapter>
  Status GenerateOutput() {
    StructArray typed_values(this->values.ToArrayData());
    Adapter adapter(this);
    // There's nothing to do for Struct except to generate the validity bitmap
    return adapter.Generate([&](int64_t index) { return Status::OK(); },
                            /*visit_null=*/VisitNoop);
  }

  Status Finish() override {
    StructArray typed_values(this->values.ToArrayData());

    // Select from children without boundschecking
    out->child_data.resize(this->values.type->num_fields());
    for (int field_index = 0; field_index < this->values.type->num_fields();
         ++field_index) {
      ARROW_ASSIGN_OR_RAISE(Datum taken_field,
                            Take(Datum(typed_values.field(field_index)),
                                 Datum(this->selection.ToArrayData()),
                                 TakeOptions::NoBoundsCheck(), ctx->exec_context()));
      out->child_data[field_index] = taken_field.array();
    }
    return Status::OK();
  }
};

#undef LIFT_BASE_MEMBERS

// ----------------------------------------------------------------------

template <typename Impl>
Status FilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  int64_t output_length =
      GetFilterOutputSize(batch[1].array, FilterState::Get(ctx).null_selection_behavior);
  Impl kernel(ctx, batch, output_length, out);
  return kernel.ExecFilter();
}

}  // namespace

Status FSBFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<FSBSelectionImpl>(ctx, batch, out);
}

Status ListFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<ListSelectionImpl<ListType>>(ctx, batch, out);
}

Status LargeListFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<ListSelectionImpl<LargeListType>>(ctx, batch, out);
}

Status FSLFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<FSLSelectionImpl>(ctx, batch, out);
}

Status DenseUnionFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<DenseUnionSelectionImpl>(ctx, batch, out);
}

Status MapFilterExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return FilterExec<ListSelectionImpl<MapType>>(ctx, batch, out);
}

namespace {

template <typename Impl>
Status TakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  if (TakeState::Get(ctx).boundscheck) {
    RETURN_NOT_OK(CheckIndexBounds(batch[1].array, batch[0].length()));
  }
  Impl kernel(ctx, batch, /*output_length=*/batch[1].length(), out);
  return kernel.ExecTake();
}

}  // namespace

Status VarBinaryTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<VarBinarySelectionImpl<BinaryType>>(ctx, batch, out);
}

Status LargeVarBinaryTakeExec(KernelContext* ctx, const ExecSpan& batch,
                              ExecResult* out) {
  return TakeExec<VarBinarySelectionImpl<LargeBinaryType>>(ctx, batch, out);
}

Status FSBTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<FSBSelectionImpl>(ctx, batch, out);
}

Status ListTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<ListSelectionImpl<ListType>>(ctx, batch, out);
}

Status LargeListTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<ListSelectionImpl<LargeListType>>(ctx, batch, out);
}

Status FSLTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<FSLSelectionImpl>(ctx, batch, out);
}

Status DenseUnionTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<DenseUnionSelectionImpl>(ctx, batch, out);
}

Status StructTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<StructSelectionImpl>(ctx, batch, out);
}

Status MapTakeExec(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  return TakeExec<ListSelectionImpl<MapType>>(ctx, batch, out);
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
