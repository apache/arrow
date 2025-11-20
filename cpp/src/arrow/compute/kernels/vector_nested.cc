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

// Vector kernels involving nested types

#include "arrow/array/array_base.h"
#include "arrow/compute/api_vector.h"
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/registry_internal.h"
#include "arrow/result.h"
#include "arrow/util/bit_run_reader.h"
#include "arrow/util/bit_util.h"
#include "arrow/util/list_util.h"
#include "arrow/util/logging_internal.h"
#include "arrow/visit_type_inline.h"

namespace arrow {

using internal::CountSetBits;
using list_util::internal::RangeOfValuesUsed;

namespace compute {
namespace internal {
namespace {

template <typename Type>
Status ListFlatten(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  auto recursive = OptionsWrapper<ListFlattenOptions>::Get(ctx).recursive;
  typename TypeTraits<Type>::ArrayType list_array(batch[0].array.ToArrayData());

  auto pool = ctx->memory_pool();
  ARROW_ASSIGN_OR_RAISE(auto result, (recursive ? list_array.FlattenRecursively(pool)
                                                : list_array.Flatten(pool)));

  out->value = std::move(result->data());
  return Status::OK();
}

struct ListParentIndicesArray {
  KernelContext* ctx;
  const std::shared_ptr<ArrayData>& input;
  int64_t base_output_offset;
  std::shared_ptr<ArrayData> out;

  template <typename Type, typename offset_type = typename Type::offset_type>
  Status VisitList(const Type&) {
    typename TypeTraits<Type>::ArrayType list(input);

    const offset_type* offsets = list.raw_value_offsets();
    offset_type values_length = offsets[list.length()] - offsets[0];

    ARROW_ASSIGN_OR_RAISE(auto indices, ctx->Allocate(values_length * sizeof(int64_t)));
    auto out_indices = reinterpret_cast<int64_t*>(indices->mutable_data());
    for (int64_t i = 0; i < list.length(); ++i) {
      // Note: In most cases, null slots are empty, but when they are non-empty
      // we write out the indices so make sure they are accounted for. This
      // behavior could be changed if needed in the future.
      for (offset_type j = offsets[i]; j < offsets[i + 1]; ++j) {
        *out_indices++ = i + base_output_offset;
      }
    }

    BufferVector buffers{nullptr, std::move(indices)};
    int64_t null_count = 0;
    out = std::make_shared<ArrayData>(int64(), values_length, std::move(buffers),
                                      null_count);
    return Status::OK();
  }

  Status Visit(const ListType& type) { return VisitList(type); }

  Status Visit(const LargeListType& type) { return VisitList(type); }

  template <typename Type, typename offset_type = typename Type::offset_type>
  Status VisitListView(const Type&) {
    ArraySpan list_view{*input};

    const offset_type* offsets = list_view.GetValues<offset_type>(1);
    const offset_type* sizes = list_view.GetValues<offset_type>(2);
    int64_t values_offset;
    int64_t values_length;
    ARROW_ASSIGN_OR_RAISE(std::tie(values_offset, values_length),
                          RangeOfValuesUsed(list_view));

    ARROW_ASSIGN_OR_RAISE(auto indices_validity,
                          AllocateEmptyBitmap(values_length, ctx->memory_pool()));
    auto* out_indices_validity = indices_validity->mutable_data();
    int64_t total_pop_count = 0;

    ARROW_ASSIGN_OR_RAISE(auto indices, ctx->Allocate(values_length * sizeof(int64_t)));
    auto* out_indices = indices->template mutable_data_as<int64_t>();
    memset(out_indices, -1, values_length * sizeof(int64_t));

    const auto* validity = list_view.GetValues<uint8_t>(0, 0);
    RETURN_NOT_OK(arrow::internal::VisitSetBitRuns(
        validity, list_view.offset, list_view.length,
        [this, offsets, sizes, out_indices, out_indices_validity, values_offset,
         &total_pop_count](int64_t run_start, int64_t run_length) {
          for (int64_t i = run_start; i < run_start + run_length; ++i) {
            auto validity_offset = offsets[i] - values_offset;
            const int64_t pop_count =
                CountSetBits(out_indices_validity, validity_offset, sizes[i]);
            if (ARROW_PREDICT_FALSE(pop_count > 0)) {
              return Status::Invalid(
                  "Function 'list_parent_indices' cannot produce parent indices for "
                  "values used by more than one list-view array element.");
            }
            bit_util::SetBitmap(out_indices_validity, validity_offset, sizes[i]);
            total_pop_count += sizes[i];
            for (auto j = static_cast<int64_t>(offsets[i]);
                 j < static_cast<int64_t>(offsets[i]) + sizes[i]; ++j) {
              out_indices[j - values_offset] = i + base_output_offset;
            }
          }
          return Status::OK();
        }));

    DCHECK_LE(total_pop_count, values_length);
    const int64_t null_count = values_length - total_pop_count;
    BufferVector buffers{null_count > 0 ? std::move(indices_validity) : nullptr,
                         std::move(indices)};
    out = std::make_shared<ArrayData>(int64(), values_length, std::move(buffers),
                                      null_count);
    return Status::OK();
  }

  Status Visit(const ListViewType& type) { return VisitListView(type); }

  Status Visit(const LargeListViewType& type) { return VisitListView(type); }

  Status Visit(const FixedSizeListType& type) {
    using offset_type = typename FixedSizeListType::offset_type;
    const offset_type slot_length = type.list_size();
    const int64_t values_length = slot_length * (input->length - input->GetNullCount());
    ARROW_ASSIGN_OR_RAISE(auto indices,
                          ctx->Allocate(values_length * sizeof(offset_type)));
    auto* out_indices = reinterpret_cast<offset_type*>(indices->mutable_data());
    const auto* bitmap = input->GetValues<uint8_t>(0, 0);
    for (int32_t i = 0; i < input->length; i++) {
      if (!bitmap || bit_util::GetBit(bitmap, input->offset + i)) {
        std::fill(out_indices, out_indices + slot_length,
                  static_cast<int32_t>(base_output_offset + i));
        out_indices += slot_length;
      }
    }
    out = ArrayData::Make(int64(), values_length, {nullptr, std::move(indices)},
                          /*null_count=*/0);
    return Status::OK();
  }

  Status Visit(const DataType& type) {
    return Status::TypeError("Function 'list_parent_indices' expects list input, got ",
                             type.ToString());
  }

  static Result<std::shared_ptr<ArrayData>> Exec(KernelContext* ctx,
                                                 const std::shared_ptr<ArrayData>& input,
                                                 int64_t base_output_offset = 0) {
    ListParentIndicesArray self{ctx, input, base_output_offset, /*out=*/nullptr};
    RETURN_NOT_OK(VisitTypeInline(*input->type, &self));
    DCHECK_NE(self.out, nullptr);
    return self.out;
  }
};

const FunctionDoc list_flatten_doc(
    "Flatten list values",
    ("`lists` must have a list-like type (lists, list-views, and\n"
     "fixed-size lists).\n"
     "Return an array with the top list level flattened unless\n"
     "`recursive` is set to true in ListFlattenOptions. When that\n"
     "is that case, flattening happens recursively until a non-list\n"
     "array is formed.\n"
     "\n"
     "Null list values do not emit anything to the output."),
    {"lists"}, "ListFlattenOptions");

const FunctionDoc list_parent_indices_doc(
    "Compute parent indices of nested list values",
    ("`lists` must have a list-like or list-view type.\n"
     "For each value in each list of `lists`, the top-level list index\n"
     "is emitted."),
    {"lists"});

class ListParentIndicesFunction : public MetaFunction {
 public:
  ListParentIndicesFunction()
      : MetaFunction("list_parent_indices", Arity::Unary(), list_parent_indices_doc) {}

  Result<Datum> ExecuteImpl(const std::vector<Datum>& args,
                            const FunctionOptions* options,
                            ExecContext* ctx) const override {
    KernelContext kernel_ctx(ctx);
    switch (args[0].kind()) {
      case Datum::ARRAY:
        return ListParentIndicesArray::Exec(&kernel_ctx, args[0].array());
      case Datum::CHUNKED_ARRAY: {
        const auto& input = args[0].chunked_array();

        int64_t base_output_offset = 0;
        ArrayVector out_chunks;
        out_chunks.reserve(input->num_chunks());
        for (const auto& chunk : input->chunks()) {
          ARROW_ASSIGN_OR_RAISE(auto out_chunk,
                                ListParentIndicesArray::Exec(&kernel_ctx, chunk->data(),
                                                             base_output_offset));
          out_chunks.push_back(MakeArray(std::move(out_chunk)));
          base_output_offset += chunk->length();
        }
        return std::make_shared<ChunkedArray>(std::move(out_chunks), int64());
      }
      default:
        return Status::NotImplemented(
            "Unsupported input type for function 'list_parent_indices': ",
            args[0].ToString());
    }
  }
};

const ListFlattenOptions* GetDefaultListFlattenOptions() {
  static const auto kDefaultListFlattenOptions = ListFlattenOptions::Defaults();
  return &kDefaultListFlattenOptions;
}

template <typename InListType>
void AddBaseListFlattenKernels(VectorFunction* func) {
  auto in_type = {InputType(InListType::type_id)};
  auto out_type = OutputType(ListValuesType);
  VectorKernel kernel(in_type, out_type, ListFlatten<InListType>,
                      OptionsWrapper<ListFlattenOptions>::Init);
  DCHECK_OK(func->AddKernel(std::move(kernel)));
}

void AddBaseListFlattenKernels(VectorFunction* func) {
  AddBaseListFlattenKernels<ListType>(func);
  AddBaseListFlattenKernels<LargeListType>(func);
  AddBaseListFlattenKernels<FixedSizeListType>(func);
  AddBaseListFlattenKernels<ListViewType>(func);
  AddBaseListFlattenKernels<LargeListViewType>(func);
}

}  // namespace

void RegisterVectorNested(FunctionRegistry* registry) {
  auto flatten = std::make_shared<VectorFunction>(
      "list_flatten", Arity::Unary(), list_flatten_doc, GetDefaultListFlattenOptions());
  AddBaseListFlattenKernels(flatten.get());
  DCHECK_OK(registry->AddFunction(std::move(flatten)));

  DCHECK_OK(registry->AddFunction(std::make_shared<ListParentIndicesFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
