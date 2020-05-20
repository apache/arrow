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

#include "arrow/compute/kernels/scalar_cast_internal.h"
#include "arrow/compute/kernels/common.h"

namespace arrow {
namespace compute {
namespace internal {

void CastFromExtension(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  // const CastOptions& options = static_cast<const CastState*>(ctx->state())->options;

  const DataType& in_type = *batch[0].type();
  const auto storage_type = checked_cast<const ExtensionType&>(in_type).storage_type();

  std::shared_ptr<const CastFunction> cast_func;
  Status s = GetCastFunction(storage_type, out->type()).Value(&cast_func);
  if (!s.ok()) {
    ctx->SetStatus(s);
    return;
  }

  // TODO: Finish implementing this

  // KERNEL_ABORT_IF_ERROR(ctx, cast_func->Execute(*batch[0]->array(),
  // out->mutable_array()));
}

std::unique_ptr<KernelState> CastInit(KernelContext* ctx, const KernelInitArgs& args) {
  // NOTE: TakeOptions are currently unused, but we pass it through anyway
  auto cast_options = static_cast<const CastOptions*>(args.options);

  // Ensure that the requested type to cast to was attached to the options
  DCHECK(cast_options->to_type);
  return std::unique_ptr<KernelState>(new CastState(*cast_options));
}

Result<ValueDescr> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<ValueDescr>& args) {
  const CastOptions& options = static_cast<const CastState&>(*ctx->state()).options;
  return ValueDescr(options.to_type, args[0].shape);
}

void ZeroCopyCastExec(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  if (batch[0].kind() == Datum::ARRAY) {
    // Make a copy of the buffers into a destination array without carrying
    // the type
    const ArrayData& input = *batch[0].array();
    ArrayData* output = out->mutable_array();
    output->length = input.length;
    output->SetNullCount(input.null_count);
    output->buffers = input.buffers;
    output->offset = input.offset;
    output->child_data = input.child_data;
  } else {
    ctx->SetStatus(
        Status::NotImplemented("This cast not yet implemented for "
                               "scalar input"));
  }
}

void AddZeroCopyCast(const std::shared_ptr<DataType>& in_type,
                     const std::shared_ptr<DataType>& out_type, CastFunction* func) {
  auto sig = KernelSignature::Make({in_type}, out_type);

  ScalarKernel kernel;
  kernel.init = CastInit;
  kernel.exec = ZeroCopyCastExec;
  kernel.signature = sig;

  // Turn off memory allocation
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;

  DCHECK_OK(func->AddKernel(in_type->id(), std::move(kernel)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
