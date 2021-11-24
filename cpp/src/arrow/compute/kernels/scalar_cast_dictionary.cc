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

// Implementation of casting to dictionary type

#include <arrow/util/bitmap_ops.h>
#include <arrow/util/checked_cast.h>

#include "arrow/array/builder_primitive.h"
#include "arrow/compute/cast_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/util_internal.h"
#include "arrow/util/int_util.h"

namespace arrow {
using internal::CopyBitmap;

namespace compute {
namespace internal {

Status CastDictionary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const CastOptions& options = CastState::Get(ctx);
  auto out_type = std::static_pointer_cast<DictionaryType>(out->type());

  // if out type is same as in type, return input
  if (out_type->Equals(batch[0].type())) {
    *out = batch[0];
    return Status::OK();
  }

  if (batch[0].is_scalar()) {  // if input is scalar
    auto in_scalar = checked_cast<const DictionaryScalar&>(*batch[0].scalar());

    // if invalid scalar, return null scalar
    if (!in_scalar.is_valid) {
      *out = MakeNullScalar(out_type);
      return Status::OK();
    }

    Datum casted_index, casted_dict;
    if (in_scalar.value.index->type->Equals(out_type->index_type())) {
      casted_index = in_scalar.value.index;
    } else {
      ARROW_ASSIGN_OR_RAISE(casted_index,
                            Cast(in_scalar.value.index, out_type->index_type(), options,
                                 ctx->exec_context()));
    }

    if (in_scalar.value.dictionary->type()->Equals(out_type->value_type())) {
      casted_dict = in_scalar.value.dictionary;
    } else {
      ARROW_ASSIGN_OR_RAISE(
          casted_dict, Cast(in_scalar.value.dictionary, out_type->value_type(), options,
                            ctx->exec_context()));
    }

    *out = std::static_pointer_cast<Scalar>(
        DictionaryScalar::Make(casted_index.scalar(), casted_dict.make_array()));

    return Status::OK();
  }

  // if input is array
  const std::shared_ptr<ArrayData>& in_array = batch[0].array();
  const auto& in_type = checked_cast<const DictionaryType&>(*in_array->type);

  ArrayData* out_array = out->mutable_array();

  if (in_type.index_type()->Equals(out_type->index_type())) {
    out_array->buffers[0] = in_array->buffers[0];
    out_array->buffers[1] = in_array->buffers[1];
    out_array->null_count = in_array->GetNullCount();
    out_array->offset = in_array->offset;
  } else {
    // for indices, create a dummy ArrayData with index_type()
    const std::shared_ptr<ArrayData>& indices_arr =
        ArrayData::Make(in_type.index_type(), in_array->length, in_array->buffers,
                        in_array->GetNullCount(), in_array->offset);
    ARROW_ASSIGN_OR_RAISE(auto casted_indices, Cast(indices_arr, out_type->index_type(),
                                                    options, ctx->exec_context()));
    out_array->buffers[0] = std::move(casted_indices.array()->buffers[0]);
    out_array->buffers[1] = std::move(casted_indices.array()->buffers[1]);
  }

  // data (dict)
  if (in_type.value_type()->Equals(out_type->value_type())) {
    out_array->dictionary = in_array->dictionary;
  } else {
    const std::shared_ptr<Array>& dict_arr = MakeArray(in_array->dictionary);
    ARROW_ASSIGN_OR_RAISE(auto casted_data, Cast(dict_arr, out_type->value_type(),
                                                 options, ctx->exec_context()));
    out_array->dictionary = casted_data.array();
  }
  return Status::OK();
}

std::vector<std::shared_ptr<CastFunction>> GetDictionaryCasts() {
  auto func = std::make_shared<CastFunction>("cast_dictionary", Type::DICTIONARY);

  AddCommonCasts(Type::DICTIONARY, kOutputTargetType, func.get());
  ScalarKernel kernel({InputType(Type::DICTIONARY)}, kOutputTargetType, CastDictionary);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

  DCHECK_OK(func->AddKernel(Type::DICTIONARY, std::move(kernel)));

  return {func};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
