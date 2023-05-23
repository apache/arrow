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

#include <memory>
#include <string>
#include <vector>

#include "arrow/compute/function.h"
#include "arrow/compute/kernel.h"
#include "arrow/compute/kernels/codegen_internal.h"
#include "arrow/compute/kernels/vector_selection_internal.h"
#include "arrow/compute/registry.h"
#include "arrow/util/logging.h"

namespace arrow {
namespace compute {
namespace internal {

void RegisterSelectionFunction(const std::string& name, FunctionDoc doc,
                               VectorKernel base_kernel, InputType selection_type,
                               const std::vector<SelectionKernelData>& kernels,
                               const FunctionOptions* default_options,
                               FunctionRegistry* registry) {
  auto func = std::make_shared<VectorFunction>(name, Arity::Binary(), std::move(doc),
                                               default_options);
  for (auto& kernel_data : kernels) {
    base_kernel.signature =
        KernelSignature::Make({std::move(kernel_data.input), selection_type}, FirstType);
    base_kernel.exec = kernel_data.exec;
    DCHECK_OK(func->AddKernel(base_kernel));
  }
  DCHECK_OK(registry->AddFunction(std::move(func)));
}

Status PreallocateData(KernelContext* ctx, int64_t length, int bit_width,
                       bool allocate_validity, ArrayData* out) {
  // Preallocate memory
  out->length = length;
  out->buffers.resize(2);

  if (allocate_validity) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[0], ctx->AllocateBitmap(length));
  }
  if (bit_width == 1) {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->AllocateBitmap(length));
  } else {
    ARROW_ASSIGN_OR_RAISE(out->buffers[1], ctx->Allocate(length * bit_width / 8));
  }
  return Status::OK();
}


}  // namespace internal
}  // namespace compute
}  // namespace arrow
