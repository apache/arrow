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

Status CastToDictionary(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const CastOptions& options = CastState::Get(ctx);
  const auto& out_type = checked_cast<const DictionaryType&>(*out->type());

  std::shared_ptr<ArrayData> in_array = batch[0].array.ToArrayData();

  // if out type is same as in type, return input
  if (out_type.Equals(*batch[0].type())) {
    /// XXX: This is the wrong place to do a zero-copy optimization
    out->value = in_array;
    return Status::OK();
  }

  // If the input type is string or binary-like, it is first encoded as a dictionary to
  // facilitate processing. This approach allows the subsequent code to uniformly handle
  // string or binary-like inputs as if they were originally provided in dictionary
  // format. Encoding as a dictionary helps in reusing the same logic for dictionary
  // operations.
  if (is_base_binary_like(in_array->type->id())) {
    in_array = DictionaryEncode(in_array)->array();
  }
  const auto& in_type = checked_cast<const DictionaryType&>(*in_array->type);

  ArrayData* out_array = out->array_data().get();

  /// XXX: again, maybe the wrong place for zero-copy optimizations
  if (in_type.index_type()->Equals(out_type.index_type())) {
    out_array->buffers[0] = in_array->buffers[0];
    out_array->buffers[1] = in_array->buffers[1];
    out_array->null_count = in_array->GetNullCount();
    out_array->offset = in_array->offset;
  } else {
    // for indices, create a dummy ArrayData with index_type()
    std::shared_ptr<ArrayData> indices_arr =
        ArrayData::Make(in_type.index_type(), in_array->length, in_array->buffers,
                        in_array->GetNullCount(), in_array->offset);
    ARROW_ASSIGN_OR_RAISE(auto casted_indices, Cast(indices_arr, out_type.index_type(),
                                                    options, ctx->exec_context()));
    out_array->buffers[0] = std::move(casted_indices.array()->buffers[0]);
    out_array->buffers[1] = std::move(casted_indices.array()->buffers[1]);
  }

  // data (dict)
  if (in_type.value_type()->Equals(out_type.value_type())) {
    out_array->dictionary = in_array->dictionary;
  } else {
    const std::shared_ptr<Array>& dict_arr = MakeArray(in_array->dictionary);
    ARROW_ASSIGN_OR_RAISE(auto casted_data, Cast(dict_arr, out_type.value_type(), options,
                                                 ctx->exec_context()));
    out_array->dictionary = casted_data.array();
  }
  return Status::OK();
}

template <typename SrcType>
void AddDictionaryCast(CastFunction* func) {
  ScalarKernel kernel({InputType(SrcType::type_id)}, kOutputTargetType, CastToDictionary);
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(SrcType::type_id, std::move(kernel)));
}

std::vector<std::shared_ptr<CastFunction>> GetDictionaryCasts() {
  auto cast_dict = std::make_shared<CastFunction>("cast_dictionary", Type::DICTIONARY);
  AddCommonCasts(Type::DICTIONARY, kOutputTargetType, cast_dict.get());
  AddDictionaryCast<DictionaryType>(cast_dict.get());
  AddDictionaryCast<StringType>(cast_dict.get());
  AddDictionaryCast<LargeStringType>(cast_dict.get());
  AddDictionaryCast<BinaryType>(cast_dict.get());
  AddDictionaryCast<LargeBinaryType>(cast_dict.get());

  return {cast_dict};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
