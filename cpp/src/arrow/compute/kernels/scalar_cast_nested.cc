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

// Implementation of casting to (or between) list types

#include <utility>
#include <vector>

#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"

namespace arrow {
namespace compute {
namespace internal {

template <typename Type>
void CastListExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

  const ArrayData& input = *batch[0].array();
  ArrayData* result = out->mutable_array();

  if (input.offset != 0) {
    ctx->SetStatus(Status::NotImplemented(
        "Casting sliced lists (non-zero offset) not yet implemented"));
    return;
  }
  // Copy buffers from parent
  result->buffers = input.buffers;

  auto child_type = checked_cast<const Type&>(*result->type).value_type();

  Datum casted_child;
  KERNEL_RETURN_IF_ERROR(
      ctx, Cast(Datum(input.child_data[0]), child_type, options, ctx->exec_context())
               .Value(&casted_child));
  DCHECK_EQ(Datum::ARRAY, casted_child.kind());
  result->child_data.push_back(casted_child.array());
}

template <typename Type>
void AddListCast(CastFunction* func) {
  ScalarKernel kernel;
  kernel.exec = CastListExec<Type>;
  kernel.signature = KernelSignature::Make({InputType(Type::type_id)}, kOutputTargetType);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(Type::type_id, std::move(kernel)));
}

std::vector<std::shared_ptr<CastFunction>> GetNestedCasts() {
  // We use the list<T> from the CastOptions when resolving the output type

  auto cast_list = std::make_shared<CastFunction>("cast_list", Type::LIST);
  AddCommonCasts(Type::LIST, kOutputTargetType, cast_list.get());
  AddListCast<ListType>(cast_list.get());

  auto cast_large_list =
      std::make_shared<CastFunction>("cast_large_list", Type::LARGE_LIST);
  AddCommonCasts(Type::LARGE_LIST, kOutputTargetType, cast_large_list.get());
  AddListCast<LargeListType>(cast_large_list.get());

  // FSL is a bit incomplete at the moment
  auto cast_fsl =
      std::make_shared<CastFunction>("cast_fixed_size_list", Type::FIXED_SIZE_LIST);
  AddCommonCasts(Type::FIXED_SIZE_LIST, kOutputTargetType, cast_fsl.get());

  // So is struct
  auto cast_struct = std::make_shared<CastFunction>("cast_struct", Type::STRUCT);
  AddCommonCasts(Type::STRUCT, kOutputTargetType, cast_struct.get());

  return {cast_list, cast_large_list, cast_fsl, cast_struct};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
