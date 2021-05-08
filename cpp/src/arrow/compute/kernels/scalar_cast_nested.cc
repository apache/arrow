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

#include "arrow/array/builder_nested.h"
#include "arrow/compute/api_scalar.h"
#include "arrow/compute/cast.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/util/bitmap_ops.h"

namespace arrow {

using internal::CopyBitmap;

namespace compute {
namespace internal {

template <typename Type>
Status CastListExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  using offset_type = typename Type::offset_type;
  using ScalarType = typename TypeTraits<Type>::ScalarType;

  const CastOptions& options = CastState::Get(ctx);

  auto child_type = checked_cast<const Type&>(*out->type()).value_type();

  if (out->kind() == Datum::SCALAR) {
    const auto& in_scalar = checked_cast<const ScalarType&>(*batch[0].scalar());
    auto out_scalar = checked_cast<ScalarType*>(out->scalar().get());

    DCHECK(!out_scalar->is_valid);
    if (in_scalar.is_valid) {
      ARROW_ASSIGN_OR_RAISE(out_scalar->value, Cast(*in_scalar.value, child_type, options,
                                                    ctx->exec_context()));

      out_scalar->is_valid = true;
    }
    return Status::OK();
  }

  const ArrayData& in_array = *batch[0].array();
  ArrayData* out_array = out->mutable_array();

  // Copy from parent
  out_array->buffers = in_array.buffers;
  Datum values = in_array.child_data[0];

  if (in_array.offset != 0) {
    if (in_array.buffers[0]) {
      ARROW_ASSIGN_OR_RAISE(out_array->buffers[0],
                            CopyBitmap(ctx->memory_pool(), in_array.buffers[0]->data(),
                                       in_array.offset, in_array.length));
    }
    ARROW_ASSIGN_OR_RAISE(out_array->buffers[1],
                          ctx->Allocate(sizeof(offset_type) * (in_array.length + 1)));

    auto offsets = in_array.GetValues<offset_type>(1);
    auto shifted_offsets = out_array->GetMutableValues<offset_type>(1);

    for (int64_t i = 0; i < in_array.length + 1; ++i) {
      shifted_offsets[i] = offsets[i] - offsets[0];
    }
    values = in_array.child_data[0]->Slice(offsets[0], offsets[in_array.length]);
  }

  ARROW_ASSIGN_OR_RAISE(Datum cast_values,
                        Cast(values, child_type, options, ctx->exec_context()));

  DCHECK_EQ(Datum::ARRAY, cast_values.kind());
  out_array->child_data.push_back(cast_values.array());
  return Status::OK();
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

  // So is dictionary
  auto cast_dictionary =
      std::make_shared<CastFunction>("cast_dictionary", Type::DICTIONARY);
  AddCommonCasts(Type::DICTIONARY, kOutputTargetType, cast_dictionary.get());

  return {cast_list, cast_large_list, cast_fsl, cast_struct, cast_dictionary};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
