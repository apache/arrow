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
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/result.h"
#include "arrow/visit_type_inline.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

template <typename Type>
Status ListFlatten(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  typename TypeTraits<Type>::ArrayType list_array(batch[0].array.ToArrayData());
  ARROW_ASSIGN_OR_RAISE(auto result, list_array.Flatten(ctx->memory_pool()));
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
    ("`lists` must have a list-like type.\n"
     "Return an array with the top list level flattened.\n"
     "Top-level null values in `lists` do not emit anything in the input."),
    {"lists"});

const FunctionDoc list_parent_indices_doc(
    "Compute parent indices of nested list values",
    ("`lists` must have a list-like type.\n"
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

}  // namespace

void RegisterVectorNested(FunctionRegistry* registry) {
  auto flatten =
      std::make_shared<VectorFunction>("list_flatten", Arity::Unary(), list_flatten_doc);
  DCHECK_OK(flatten->AddKernel({Type::LIST}, OutputType(ListValuesType),
                               ListFlatten<ListType>));
  DCHECK_OK(flatten->AddKernel({Type::FIXED_SIZE_LIST}, OutputType(ListValuesType),
                               ListFlatten<FixedSizeListType>));
  DCHECK_OK(flatten->AddKernel({Type::LARGE_LIST}, OutputType(ListValuesType),
                               ListFlatten<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(flatten)));

  DCHECK_OK(registry->AddFunction(std::make_shared<ListParentIndicesFunction>()));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
