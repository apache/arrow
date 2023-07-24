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

// Implementation of casting to extension types
#include "arrow/compute/kernels/common_internal.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/scalar.h"

namespace arrow {
namespace compute {
namespace internal {

namespace {
Status CastToExtension(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const CastOptions& options = checked_cast<const CastState*>(ctx->state())->options;
  const auto& ext_ty = static_cast<const ExtensionType&>(*options.to_type.type);
  auto out_ty = ext_ty.storage_type();

  DCHECK(batch[0].is_array());
  std::shared_ptr<Array> array = batch[0].array.ToArray();

  // Try to prevent user errors by preventing casting between extensions w/
  // different storage types. Provide a tip on how to accomplish same outcome.
  std::shared_ptr<Array> result;
  if (array->type()->id() == Type::EXTENSION) {
    if (!array->type()->Equals(out_ty)) {
      return Status::TypeError("Casting from '" + array->type()->ToString() +
                               "' to different extension type '" + ext_ty.ToString() +
                               "' not permitted. One can first cast to the storage "
                               "type, then to the extension type.");
    }
    result = array;
  } else {
    ARROW_ASSIGN_OR_RAISE(result, Cast(*array, out_ty, options, ctx->exec_context()));
  }

  ExtensionArray extension(options.to_type.GetSharedPtr(), result);
  out->value = std::move(extension.data());
  return Status::OK();
}

std::shared_ptr<CastFunction> GetCastToExtension(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), Type::EXTENSION);
  for (Type::type in_ty : AllTypeIds()) {
    DCHECK_OK(
        func->AddKernel(in_ty, {InputType(in_ty)}, kOutputTargetType, CastToExtension));
  }
  return func;
}

};  // namespace

std::vector<std::shared_ptr<CastFunction>> GetExtensionCasts() {
  auto func = GetCastToExtension("cast_extension");
  return {func};
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
