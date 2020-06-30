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

template <typename Type>
void ListFlatten(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  typename TypeTraits<Type>::ArrayType list_array(batch[0].array());
  Result<std::shared_ptr<Array>> result = list_array.Flatten(ctx->memory_pool());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }
  out->value = (*result)->data();
}

static Result<ValueDescr> ValuesType(KernelContext*,
                                     const std::vector<ValueDescr>& args) {
  const auto& list_type = checked_cast<const BaseListType&>(*args[0].type);
  return ValueDescr::Array(list_type.value_type());
}

void RegisterVectorNested(FunctionRegistry* registry) {
  auto flatten = std::make_shared<VectorFunction>("list_flatten", Arity::Unary());
  DCHECK_OK(flatten->AddKernel({InputType(Type::LIST)}, OutputType(ValuesType),
                               ListFlatten<ListType>));
  DCHECK_OK(flatten->AddKernel({InputType(Type::LARGE_LIST)}, OutputType(ValuesType),
                               ListFlatten<LargeListType>));
  DCHECK_OK(registry->AddFunction(std::move(flatten)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
