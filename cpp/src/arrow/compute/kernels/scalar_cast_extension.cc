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
#include "arrow/compute/kernels/common.h"
#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/scalar.h"

namespace arrow {
namespace compute {
namespace internal {

Status CastToExtension(KernelContext* ctx, const ExecSpan& batch, ExecResult* out) {
  const CastOptions& options = checked_cast<const CastState*>(ctx->state())->options;
  auto out_ty = static_cast<const ExtensionType&>(*options.to_type.type).storage_type();
  
  DCHECK(batch[0].is_array());
  std::shared_ptr<Array> array = batch[0].array.ToArray();
  std::shared_ptr<Array> result;
  
  RETURN_NOT_OK(Cast(*array, out_ty, options,
                     ctx->exec_context())
                    .Value(&result));
  ExtensionArray extension(options.to_type.GetSharedPtr(), result);
  out->value = std::move(extension.data());
  return Status::OK();
}

std::shared_ptr<CastFunction> GetCastToExtension(std::string name) {
  auto func = std::make_shared<CastFunction>(std::move(name), Type::EXTENSION);
  // TODO(milesgranger): Better way to add all types? `AllTypeIds` exists in tests...
  for (auto types : {IntTypes(), FloatingPointTypes(), StringTypes(), BinaryTypes()}) {
    for (auto in_ty : types) {
        DCHECK_OK(func->AddKernel(in_ty->id(), {in_ty}, 
                                  kOutputTargetType, CastToExtension));
    }
  }
  DCHECK_OK(func->AddKernel(Type::DICTIONARY, {InputType(Type::DICTIONARY)},
                            kOutputTargetType, CastToExtension));
  return func;
}

std::vector<std::shared_ptr<CastFunction>> GetExtensionCasts() {
  auto func = GetCastToExtension("cast_extension");
  return {func};
}


}  // namespace internal
}  // namespace compute
}  // namespace arrow

