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
#include "arrow/compute/cast_internal.h"
#include "arrow/compute/kernels/common.h"
#include "arrow/extension_type.h"

namespace arrow {
namespace compute {
namespace internal {

void UnpackDictionary(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  DictionaryArray dict_arr(batch[0].array());
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;

  const auto& dict_type = *dict_arr.dictionary()->type();
  if (!dict_type.Equals(options.to_type)) {
    ctx->SetStatus(Status::Invalid("Cast type ", options.to_type->ToString(),
                                   " incompatible with dictionary type ",
                                   dict_type.ToString()));
    return;
  }

  Result<Datum> result = Take(Datum(dict_arr.dictionary()), Datum(dict_arr.indices()),
                              /*options=*/TakeOptions::Defaults(), ctx->exec_context());
  if (!result.ok()) {
    ctx->SetStatus(result.status());
    return;
  }
  *out = *result;
}

void OutputAllNull(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  ArrayData* output = out->mutable_array();
  output->buffers = {nullptr};
  output->null_count = batch.length;
}

void CastFromExtension(KernelContext* ctx, const ExecBatch& batch, Datum* out) {
  const CastOptions& options = checked_cast<const CastState*>(ctx->state())->options;

  const DataType& in_type = *batch[0].type();
  const auto storage_type = checked_cast<const ExtensionType&>(in_type).storage_type();

  ExtensionArray extension(batch[0].array());

  Datum casted_storage;
  KERNEL_RETURN_IF_ERROR(
      ctx, Cast(*extension.storage(), out->type(), options, ctx->exec_context())
               .Value(&casted_storage));
  out->value = casted_storage.array();
}

Result<ValueDescr> ResolveOutputFromOptions(KernelContext* ctx,
                                            const std::vector<ValueDescr>& args) {
  const CastOptions& options = checked_cast<const CastState&>(*ctx->state()).options;
  return ValueDescr(options.to_type, args[0].shape);
}

/// You will see some of kernels with
///
/// kOutputTargetType
///
/// for their output type resolution. This is somewhat of an eyesore but the
/// easiest initial way to get the requested cast type including the TimeUnit
/// to the kernel (which is needed to compute the output) was through
/// CastOptions

OutputType kOutputTargetType(ResolveOutputFromOptions);

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

void AddZeroCopyCast(Type::type in_type_id, InputType in_type, OutputType out_type,
                     CastFunction* func) {
  auto sig = KernelSignature::Make({in_type}, out_type);
  ScalarKernel kernel;
  kernel.exec = ZeroCopyCastExec;
  kernel.signature = sig;
  kernel.null_handling = NullHandling::COMPUTED_NO_PREALLOCATE;
  kernel.mem_allocation = MemAllocation::NO_PREALLOCATE;
  DCHECK_OK(func->AddKernel(in_type_id, std::move(kernel)));
}

}  // namespace internal
}  // namespace compute
}  // namespace arrow
