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
#include "arrow/compute/kernels/common.h"
#include "arrow/result.h"

namespace arrow {
namespace compute {
namespace internal {
namespace {

template <typename Type>
Status ListFlatten(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  typename TypeTraits<Type>::ArrayType list_array(batch[0].array());
  ARROW_ASSIGN_OR_RAISE(auto result, list_array.Flatten(ctx->memory_pool()));
  out->value = result->data();
  return Status::OK();
}

template <typename Type, typename offset_type = typename Type::offset_type>
Status ListParentIndices(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  typename TypeTraits<Type>::ArrayType list(batch[0].array());
  ArrayData* out_arr = out->mutable_array();

  const offset_type* offsets = list.raw_value_offsets();
  offset_type values_length = offsets[list.length()] - offsets[0];

  out_arr->length = values_length;
  out_arr->null_count = 0;
  ARROW_ASSIGN_OR_RAISE(out_arr->buffers[1],
                        ctx->Allocate(values_length * sizeof(offset_type)));
  auto out_indices = reinterpret_cast<offset_type*>(out_arr->buffers[1]->mutable_data());
  for (int64_t i = 0; i < list.length(); ++i) {
    // Note: In most cases, null slots are empty, but when they are non-empty
    // we write out the indices so make sure they are accounted for. This
    // behavior could be changed if needed in the future.
    for (offset_type j = offsets[i]; j < offsets[i + 1]; ++j) {
      *out_indices++ = static_cast<offset_type>(i);
    }
  }
  return Status::OK();
}

Result<ValueDescr> ValuesType(KernelContext*, const std::vector<ValueDescr>& args) {
  const auto& list_type = checked_cast<const BaseListType&>(*args[0].type);
  return ValueDescr::Array(list_type.value_type());
}

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

}  // namespace

void RegisterVectorNested(FunctionRegistry* registry) {
  auto flatten =
      std::make_shared<VectorFunction>("list_flatten", Arity::Unary(), &list_flatten_doc);
  DCHECK_OK(flatten->AddKernel({InputType::Array(Type::LIST)}, OutputType(ValuesType),
                               ListFlatten<ListType>));
  DCHECK_OK(flatten->AddKernel({InputType::Array(Type::LARGE_LIST)},
                               OutputType(ValuesType), ListFlatten<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(flatten)));

  auto list_parent_indices = std::make_shared<VectorFunction>(
      "list_parent_indices", Arity::Unary(), &list_parent_indices_doc);
  DCHECK_OK(list_parent_indices->AddKernel({InputType::Array(Type::LIST)}, int32(),
                                           ListParentIndices<ListType>));
  DCHECK_OK(list_parent_indices->AddKernel({InputType::Array(Type::LARGE_LIST)}, int64(),
                                           ListParentIndices<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(list_parent_indices)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
